import 'dart:async';
import 'dart:convert';

import 'package:signalr_classic/signalr_classic.dart';
import 'package:test/test.dart';

import 'support/fake_server.dart';
import 'support/recorder.dart';

const fast = Duration(milliseconds: 50);

HubConnection newConnection(
  FakeServer server, {
  TransportType transport = TransportType.auto,
  String clientProtocol = '1.5',
  bool useDefaultPath = true,
  Duration? invocationTimeout,
  Duration? transportConnectTimeout,
  String? queryString = 'session=abc 123',
  Map<String, String>? queryParameters,
}) =>
    HubConnection(
      useDefaultPath ? server.baseUrl : '${server.baseUrl}/signalr',
      options: HubConnectionOptions(
        transport: transport,
        clientProtocol: clientProtocol,
        useDefaultPath: useDefaultPath,
        queryString: queryString,
        queryParameters: queryParameters,
        headers: const <String, String>{'x-test': 'yes'},
        reconnectDelay: fast,
        httpTimeout: const Duration(seconds: 3),
        invocationTimeout: invocationTimeout,
        transportConnectTimeout: transportConnectTimeout,
      ),
    );

void main() {
  const transports = <TransportType>[
    TransportType.webSockets,
    TransportType.serverSentEvents,
    TransportType.longPolling,
  ];

  for (final t in transports) {
    group('${t.name}:', () {
      late FakeServer server;
      late HubConnection connection;
      late HubProxy hub;
      late Recorder rec;

      setUp(() async {
        server = FakeServer();
        await server.start();
        connection = newConnection(server, transport: t);
        hub = connection.createHubProxy('MarketHub');
        rec = Recorder(connection);
      });

      tearDown(() async {
        await connection.dispose();
        await rec.cancel();
        await server.close();
      });

      test('start connects and reports state, transport, protocol and id', () async {
        await connection.start();
        expect(connection.state, ConnectionState.connected);
        expect(connection.transport, t);
        expect(connection.protocolVersion, '1.5');
        expect(connection.connectionId, 'cid-1');
        await rec.waitFor('state:connected');
        expect(rec.events.take(2), <String>['state:connecting', 'state:connected']);
      });

      test('requests use <url>/signalr/<action>, protocol, connection data, token, query and headers', () async {
        await connection.start();
        final neg = server.last('negotiate');
        expect(neg.uri.path, '/app/signalr/negotiate');
        expect(neg.query['clientProtocol'], '1.5');
        expect(jsonDecode(neg.query['connectionData']!), <dynamic>[
          <String, dynamic>{'name': 'markethub'}
        ]);
        expect(neg.query['session'], 'abc 123');
        final con = server.last('connect');
        expect(con.query['transport'], t.name);
        expect(con.query['connectionToken'], 'tok+/=AQ');
        expect(int.parse(con.query['tid']!), inInclusiveRange(0, 10));
        expect(server.of('start'), hasLength(1));
        expect(server.requests.every((r) => r.testHeader == 'yes' || r.action == 'connect'), isTrue);
      });

      test('receives hub messages: case-insensitive hub and method, all arguments decoded', () async {
        final got = <List<dynamic>>[];
        hub.on('PriceUpdate', got.add);
        await connection.start();
        server.push('marketHUB', 'priceupdate', <dynamic>[
          '1=ABC,2=10.5',
          42,
          <String, dynamic>{'k': 'v'},
          'عربي'
        ]);
        await rec.waitUntil(() => got.isNotEmpty, what: 'message');
        expect(got.single, <dynamic>[
          '1=ABC,2=10.5',
          42,
          <String, dynamic>{'k': 'v'},
          'عربي'
        ]);
        expect(connection.messageId, 'd-1');
      });

      test('several messages in one frame keep their order; unknown hubs and methods are ignored', () async {
        final got = <String>[];
        hub.on('A', (a) => got.add('A${a.first}'));
        hub.on('B', (a) => got.add('B${a.first}'));
        await connection.start();
        server.pushMany(<Map<String, dynamic>>[
          <String, dynamic>{
            'H': 'MarketHub',
            'M': 'A',
            'A': <dynamic>[1]
          },
          <String, dynamic>{
            'H': 'OtherHub',
            'M': 'A',
            'A': <dynamic>[9]
          },
          <String, dynamic>{
            'H': 'MarketHub',
            'M': 'Unknown',
            'A': <dynamic>[9]
          },
          <String, dynamic>{
            'H': 'MarketHub',
            'M': 'B',
            'A': <dynamic>[2]
          },
          <String, dynamic>{
            'H': 'MarketHub',
            'M': 'A',
            'A': <dynamic>[3]
          },
        ]);
        await rec.waitUntil(() => got.length == 3, what: '3 messages');
        expect(got, <String>['A1', 'B2', 'A3']);
      });

      test('multiple handlers, off(), and a throwing handler does not break the connection', () async {
        final first = <dynamic>[];
        final second = <dynamic>[];
        void secondHandler(List<dynamic> a) => second.add(a.first);
        hub.on('M', (a) => throw StateError('boom'));
        hub.on('M', (a) => first.add(a.first));
        hub.on('M', secondHandler);
        await connection.start();
        server.push('MarketHub', 'M', <dynamic>[1]);
        await rec.waitUntil(() => first.length == 1 && second.length == 1);
        expect(rec.errors.single.kind, ErrorKind.handler);
        hub.off('M', secondHandler);
        server.push('MarketHub', 'M', <dynamic>[2]);
        await rec.waitUntil(() => first.length == 2);
        expect(second, <dynamic>[1]);
        expect(connection.state, ConnectionState.connected);
      });

      test('invoke sends H/M/A/I and returns the result', () async {
        server.onInvoke = (h, m, a, s) => Reply(result: <String, dynamic>{'echo': a});
        await connection.start();
        final r = await hub.invoke('PriceUpdate', <dynamic>['{"isin":"AE1"}', 7]);
        expect(r, <String, dynamic>{
          'echo': <dynamic>['{"isin":"AE1"}', 7]
        });
        final inv = server.invocations.single;
        expect(inv['H'], 'MarketHub');
        expect(inv['M'], 'PriceUpdate');
        expect(inv['I'], 0);
      });

      test('100 concurrent invocations resolve to their own results', () async {
        server.onInvoke = (h, m, a, s) => Reply(result: a.first);
        await connection.start();
        final results = await Future.wait(<Future<dynamic>>[
          for (var i = 0; i < 100; i++) hub.invoke('Echo', <dynamic>[i]),
        ]);
        expect(results, List<int>.generate(100, (i) => i));
      });

      test('hub errors become HubException with H and D', () async {
        server.onInvoke =
            (h, m, a, s) => Reply(error: 'Not allowed', isHubError: true, errorData: <String, dynamic>{'code': 7});
        await connection.start();
        await expectLater(
          hub.invoke('X'),
          throwsA(isA<HubException>()
              .having((e) => e.message, 'message', 'Not allowed')
              .having((e) => e.isHubException, 'isHubException', isTrue)
              .having((e) => e.errorData, 'errorData', <String, dynamic>{'code': 7})),
        );
      });

      test('hub state is sent and updated from results and messages', () async {
        server.onInvoke =
            (h, m, a, s) => Reply(result: null, state: <String, dynamic>{'count': ((s?['count'] as int?) ?? 0) + 1});
        await connection.start();
        hub.state['count'] = 1;
        await hub.invoke('Inc');
        expect(server.invocations.last['S'], <String, dynamic>{'count': 1});
        expect(hub.state['count'], 2);
        server.push('MarketHub', 'Noop', const <dynamic>[], state: <String, dynamic>{'tier': 'gold'});
        await rec.waitUntil(() => hub.state['tier'] == 'gold');
      });

      test('large payloads (1 MB) are delivered intact', () async {
        final got = <String>[];
        hub.on('Big', (a) => got.add(a.first as String));
        await connection.start();
        final big = List<String>.generate(100000, (i) => '1=${i % 10}').join('|');
        server.push('MarketHub', 'Big', <dynamic>[big]);
        await rec.waitUntil(() => got.isNotEmpty, timeout: const Duration(seconds: 10));
        expect(got.single, big);
      });

      test('dropped transport: reconnecting -> reconnected with messageId and groupsToken', () async {
        final got = <dynamic>[];
        hub.on('M', (a) => got.add(a.first));
        await connection.start();
        server.sendFrame(<String, dynamic>{
          'G': 'grp-token',
          'C': 'd-7',
          'M': <dynamic>[
            <String, dynamic>{
              'H': 'MarketHub',
              'M': 'M',
              'A': <dynamic>['before']
            }
          ]
        });
        await rec.waitUntil(() => got.length == 1);
        await server.dropTransport();
        await rec.waitFor('reconnected');
        expect(rec.events,
            containsAllInOrder(<String>['state:reconnecting', 'reconnecting', 'state:connected', 'reconnected']));
        final reconnect = server.last('reconnect');
        expect(reconnect.query['messageId'], 'd-7');
        expect(reconnect.query['groupsToken'], 'grp-token');
        server.push('MarketHub', 'M', <dynamic>['after']);
        await rec.waitUntil(() => got.length == 2);
        expect(got, <dynamic>['before', 'after']);
        expect(connection.state, ConnectionState.connected);
      });

      test('stop: abort is sent, state is disconnected, closed has no error', () async {
        await connection.start();
        await connection.stop();
        expect(connection.state, ConnectionState.disconnected);
        await rec.waitFor('closed');
        expect(rec.closedErrors.single, isNull);
        await rec.waitUntil(() => server.of('abort').isNotEmpty, what: 'abort');
        expect(server.last('abort').query['transport'], t.name);
      });

      test('pending invocation fails with ConnectionClosedException on stop', () async {
        server.silentInvocations = true;
        await connection.start();
        final f = hub.invoke('Hang');
        await Future<void>.delayed(const Duration(milliseconds: 100));
        await connection.stop();
        await expectLater(f, throwsA(isA<ConnectionClosedException>()));
      });

      test('pending invocation fails when the transport drops (never hangs)', () async {
        server.silentInvocations = true;
        await connection.start();
        final f = hub.invoke('Hang');
        await Future<void>.delayed(const Duration(milliseconds: 100));
        await server.dropTransport();
        await expectLater(f.timeout(const Duration(seconds: 5)), throwsA(isA<SignalRException>()));
      });

      test('server D:1 closes the connection without reconnecting', () async {
        await connection.start();
        server.sendFrame(<String, dynamic>{'D': 1});
        await rec.waitFor('closed');
        expect(rec.count('reconnecting'), 0);
        expect(connection.state, ConnectionState.disconnected);
      });

      test('server T:1 makes the client reconnect', () async {
        await connection.start();
        server.sendFrame(<String, dynamic>{'T': 1, 'C': 'd-3', 'M': <dynamic>[]});
        await rec.waitUntil(() => server.of('reconnect').isNotEmpty, what: '/reconnect request');
        await rec.waitUntil(() => connection.state == ConnectionState.connected);
      });

      test('can start again after stop (fresh negotiate)', () async {
        await connection.start();
        await connection.stop();
        await connection.start();
        expect(connection.state, ConnectionState.connected);
        expect(server.of('negotiate'), hasLength(2));
      });
    });
  }

  group('negotiation and protocol', () {
    late FakeServer server;
    late HubConnection connection;
    late Recorder rec;

    Future<void> setUpWith(FakeServer s,
        {TransportType transport = TransportType.auto,
        String clientProtocol = '1.5',
        bool useDefaultPath = true}) async {
      server = s;
      await server.start();
      connection =
          newConnection(server, transport: transport, clientProtocol: clientProtocol, useDefaultPath: useDefaultPath);
      connection.createHubProxy('markethub');
      rec = Recorder(connection);
    }

    tearDown(() async {
      await connection.dispose();
      await rec.cancel();
      await server.close();
    });

    test('useDefaultPath=false uses the URL as given', () async {
      await setUpWith(FakeServer(), useDefaultPath: false);
      await connection.start();
      expect(server.last('negotiate').uri.path, '/app/signalr/negotiate');
    });

    test('queryParameters are URL-encoded and sent with every request', () async {
      server = FakeServer();
      await server.start();
      connection = HubConnection(server.baseUrl,
          options: const HubConnectionOptions(queryParameters: <String, String>{'a b': 'c&d'}));
      connection.createHubProxy('h');
      rec = Recorder(connection);
      await connection.start();
      expect(server.last('negotiate').query['a b'], 'c&d');
      expect(server.last('connect').query['a b'], 'c&d');
    });

    for (final v in <String>['1.3', '1.2']) {
      test('server on protocol $v: client adapts and skips /start', () async {
        await setUpWith(FakeServer(protocolVersion: v));
        await connection.start();
        expect(connection.protocolVersion, v);
        expect(server.of('start'), isEmpty);
        expect(server.last('connect').query['clientProtocol'], v);
      });
    }

    test('server on protocol 1.4 uses /start', () async {
      await setUpWith(FakeServer(protocolVersion: '1.4'));
      await connection.start();
      expect(server.of('start'), hasLength(1));
    });

    test('unsupported protocol fails with UnsupportedProtocolException', () async {
      await setUpWith(FakeServer(protocolVersion: '1.1'));
      await expectLater(connection.start(), throwsA(isA<UnsupportedProtocolException>()));
      expect(connection.state, ConnectionState.disconnected);
      await rec.waitFor('closed');
      expect(rec.errors.single.kind, ErrorKind.connection);
    });

    test('negotiate 401 fails fast with HttpStatusException (auth error)', () async {
      await setUpWith(FakeServer()..negotiateStatus = 401);
      await expectLater(
          connection.start(), throwsA(isA<HttpStatusException>().having((e) => e.isAuthError, 'auth', isTrue)));
      await rec.waitFor('closed');
      expect(server.of('negotiate'), hasLength(1));
      expect(rec.events, <String>['state:connecting', 'error:connection', 'state:disconnected', 'closed']);
    });

    test('negotiate with invalid JSON fails with NegotiationException', () async {
      await setUpWith(FakeServer()..negotiateBodyOverride = '<html>proxy error</html>');
      await expectLater(connection.start(), throwsA(isA<NegotiationException>()));
    });

    test('negotiate without ConnectionToken fails with NegotiationException', () async {
      await setUpWith(FakeServer()..negotiateBodyOverride = '{"ProtocolVersion":"1.5"}');
      await expectLater(connection.start(), throwsA(isA<NegotiationException>()));
    });

    test('/start failure fails the connection and sends abort', () async {
      await setUpWith(FakeServer()..startStatus = 500);
      await expectLater(connection.start(), throwsA(isA<HttpStatusException>()));
      expect(connection.state, ConnectionState.disconnected);
      await rec.waitUntil(() => server.of('abort').isNotEmpty, what: 'abort');
    });

    test('createHubProxy after start throws; same name returns the same proxy', () async {
      await setUpWith(FakeServer());
      expect(identical(connection.createHubProxy('MarketHub'), connection.createHubProxy('markethub')), isTrue);
      await connection.start();
      expect(() => connection.createHubProxy('other'), throwsA(isA<InvalidStateException>()));
    });
  });

  group('automatic transport selection', () {
    late FakeServer server;
    late HubConnection connection;

    tearDown(() async {
      await connection.dispose();
      await server.close();
    });

    Future<void> run(FakeServer s, TransportType expected) async {
      server = s;
      await server.start();
      connection = newConnection(server, transportConnectTimeout: const Duration(seconds: 1));
      connection.createHubProxy('h');
      await connection.start();
      expect(connection.transport, expected);
    }

    test('prefers WebSockets', () => run(FakeServer(), TransportType.webSockets));
    test('TryWebSockets=false -> Server-Sent Events',
        () => run(FakeServer(tryWebSockets: false), TransportType.serverSentEvents));
    test('WebSocket upgrade fails -> Server-Sent Events',
        () => run(FakeServer()..allowWebSockets = false, TransportType.serverSentEvents));
    test('WebSockets and SSE fail -> Long Polling', () {
      return run(
          FakeServer()
            ..allowWebSockets = false
            ..allowServerSentEvents = false,
          TransportType.longPolling);
    });
    test('SSE without the "initialized" event still connects on the first data', () {
      return run(
          FakeServer(tryWebSockets: false)
            ..sendInitMessage = true
            ..sendSseInitialized = false
            ..allowServerSentEvents = true,
          TransportType.serverSentEvents);
    });

    test('every transport fails -> TransportException and disconnected', () async {
      server = FakeServer()
        ..allowWebSockets = false
        ..allowServerSentEvents = false
        ..allowLongPolling = false;
      await server.start();
      connection = newConnection(server, transportConnectTimeout: const Duration(seconds: 1));
      connection.createHubProxy('h');
      await expectLater(connection.start(), throwsA(isA<TransportException>()));
      expect(connection.state, ConnectionState.disconnected);
    });

    test('explicit transport does not fall back', () async {
      server = FakeServer()..allowWebSockets = false;
      await server.start();
      connection = newConnection(server, transport: TransportType.webSockets);
      connection.createHubProxy('h');
      await expectLater(connection.start(), throwsA(isA<TransportException>()));
      expect(server.requests.where((r) => r.query['transport'] == 'serverSentEvents'), isEmpty);
    });

    test('no init message on any transport -> TransportException after trying all three', () async {
      server = FakeServer()..sendInitMessage = false;
      await server.start();
      connection = newConnection(server, transportConnectTimeout: const Duration(milliseconds: 500));
      connection.createHubProxy('h');
      // WS opens but never sends S:1; SSE also lacks S:1; long polling init has S:0.
      await expectLater(connection.start(), throwsA(isA<TransportException>()));
    });
  });

  group('keep-alive, slow connection and reconnect window', () {
    late FakeServer server;
    late HubConnection connection;
    late Recorder rec;

    Future<void> setUpWith(FakeServer s, TransportType t) async {
      server = s;
      await server.start();
      connection = newConnection(server, transport: t);
      connection.createHubProxy('h');
      rec = Recorder(connection);
    }

    tearDown(() async {
      await connection.dispose();
      await rec.cancel();
      await server.close();
    });

    for (final t in <TransportType>[TransportType.webSockets, TransportType.serverSentEvents]) {
      test('${t.name}: silence -> slow at 2/3 of KeepAliveTimeout, then reconnecting', () async {
        await setUpWith(FakeServer(keepAliveSeconds: 0.6, disconnectSeconds: 5), t);
        await connection.start();
        await rec.waitFor('slow', timeout: const Duration(seconds: 2));
        await rec.waitFor('reconnecting', timeout: const Duration(seconds: 2));
        expect(rec.events.indexOf('slow'), lessThan(rec.events.indexOf('reconnecting')));
        await rec.waitFor('reconnected');
      });

      test('${t.name}: keep-alives prevent slow and reconnect', () async {
        await setUpWith(FakeServer(keepAliveSeconds: 0.6, disconnectSeconds: 5), t);
        await connection.start();
        for (var i = 0; i < 10; i++) {
          server.keepAlive();
          await Future<void>.delayed(const Duration(milliseconds: 120));
        }
        expect(rec.count('slow'), 0);
        expect(rec.count('reconnecting'), 0);
      });
    }

    test('KeepAliveTimeout null disables keep-alive monitoring', () async {
      await setUpWith(FakeServer(keepAliveSeconds: null, disconnectSeconds: 5), TransportType.webSockets);
      await connection.start();
      await Future<void>.delayed(const Duration(milliseconds: 800));
      expect(rec.count('slow'), 0);
      expect(connection.state, ConnectionState.connected);
    });

    test('long polling has no keep-alive: quiet server stays connected', () async {
      await setUpWith(FakeServer(keepAliveSeconds: 0.3, disconnectSeconds: 5, connectionTimeoutSeconds: 0.2),
          TransportType.longPolling);
      await connection.start();
      await Future<void>.delayed(const Duration(milliseconds: 900));
      expect(rec.count('slow'), 0);
      expect(rec.count('reconnecting'), 0);
      expect(server.of('poll').length, greaterThan(1));
    });

    test('reconnect keeps failing -> closed with error after the reconnect window', () async {
      await setUpWith(FakeServer(keepAliveSeconds: null, disconnectSeconds: 0.5), TransportType.webSockets);
      await connection.start();
      server.rejectReconnect = true;
      await server.dropTransport();
      await rec.waitFor('closed', timeout: const Duration(seconds: 3));
      expect(rec.closedErrors.single, isA<SignalRException>());
      expect(server.of('reconnect').length, greaterThan(1));
    });

    test('reconnect rejected with 403 -> closed at once (no retry loop)', () async {
      await setUpWith(FakeServer(disconnectSeconds: 30), TransportType.serverSentEvents);
      await connection.start();
      server
        ..rejectReconnect = true
        ..rejectReconnectStatus = 403;
      await server.dropTransport();
      await rec.waitFor('closed');
      expect(server.of('reconnect'), hasLength(1));
      expect(rec.closedErrors.single, isA<HttpStatusException>());
    });
  });

  group('lifecycle', () {
    late FakeServer server;
    late HubConnection connection;
    late Recorder rec;

    tearDown(() async {
      await connection.dispose();
      await rec.cancel();
      await server.close();
    });

    Future<void> setUpWith(FakeServer s, {Duration? invocationTimeout}) async {
      server = s;
      await server.start();
      connection = newConnection(server, transport: TransportType.webSockets, invocationTimeout: invocationTimeout);
      connection.createHubProxy('h');
      rec = Recorder(connection);
    }

    test('stop during start: start fails with InvalidStateException, nothing connects later', () async {
      await setUpWith(FakeServer()..negotiateDelay = const Duration(milliseconds: 300));
      final f = connection.start();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await connection.stop();
      await expectLater(f, throwsA(isA<InvalidStateException>()));
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(connection.state, ConnectionState.disconnected);
      expect(rec.count('state:connected'), 0);
    });

    test('stop during reconnecting ends the reconnect loop', () async {
      await setUpWith(FakeServer(disconnectSeconds: 30));
      await connection.start();
      server.rejectReconnect = true;
      await server.dropTransport();
      await rec.waitFor('reconnecting');
      await connection.stop();
      final attempts = server.of('reconnect').length;
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(server.of('reconnect').length, attempts);
      expect(connection.state, ConnectionState.disconnected);
    });

    test('concurrent start() calls share one attempt; start when connected is a no-op', () async {
      await setUpWith(FakeServer());
      await Future.wait(<Future<void>>[connection.start(), connection.start(), connection.start()]);
      await connection.start();
      expect(server.of('negotiate'), hasLength(1));
    });

    test('stop when already disconnected is safe; dispose is idempotent', () async {
      await setUpWith(FakeServer());
      await connection.stop();
      await connection.dispose();
      await connection.dispose();
      expect(connection.start(), throwsA(isA<InvalidStateException>()));
    });

    test('invoke while disconnected fails with InvalidStateException', () async {
      await setUpWith(FakeServer());
      expect(connection.createHubProxy('h').invoke('X'), throwsA(isA<InvalidStateException>()));
    });

    test('invocation timeout', () async {
      await setUpWith(FakeServer()..silentInvocations = true, invocationTimeout: const Duration(milliseconds: 200));
      await connection.start();
      await expectLater(connection.createHubProxy('h').invoke('Slow'), throwsA(isA<InvocationTimeoutException>()));
    });

    test('non-JSON arguments fail without sending', () async {
      await setUpWith(FakeServer());
      await connection.start();
      expect(connection.createHubProxy('h').invoke('X', <dynamic>[Object()]), throwsA(isA<ArgumentError>()));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(server.invocations, isEmpty);
    });

    test('progress updates reach onProgress before the result', () async {
      await setUpWith(FakeServer()..onInvoke = (h, m, a, s) => Reply(result: 'done', progress: <dynamic>[10, 50, 100]));
      await connection.start();
      final progress = <dynamic>[];
      final r = await connection.createHubProxy('h').invoke('Work', const <dynamic>[], progress.add);
      expect(r, 'done');
      expect(progress, <dynamic>[10, 50, 100]);
    });

    test('invalid JSON from the server is reported as a protocol error, connection stays up', () async {
      await setUpWith(FakeServer());
      await connection.start();
      server.sendRawText('not json {');
      await rec.waitFor('error:protocol');
      expect(connection.state, ConnectionState.connected);
    });

    test('rapid start/stop cycles stay consistent', () async {
      await setUpWith(FakeServer());
      for (var i = 0; i < 10; i++) {
        final f = connection.start().catchError((Object _) {});
        if (i.isEven) await Future<void>.delayed(const Duration(milliseconds: 5));
        await connection.stop();
        await f;
      }
      await connection.start();
      expect(connection.state, ConnectionState.connected);
      await connection.stop();
      expect(connection.state, ConnectionState.disconnected);
    });
  });
}
