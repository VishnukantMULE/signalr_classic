import 'dart:async';

import 'package:signalr_classic/signalr_flutter_compat.dart';
import 'package:test/test.dart';

import 'support/fake_server.dart';

class CompatRecorder {
  final List<String> statuses = <String>[];
  final List<List<String>> messages = <List<String>>[];
  final List<String?> errors = <String?>[];

  // Apps written for signalr_flutter compare these strings, e.g. 'ConnectionStatus.connected'.
  void onStatus(dynamic s) => statuses.add('$s');
  void onMessage(String m, String a) => messages.add(<String>[m, a]);
  void onError(String? e) => errors.add(e);

  Future<void> waitUntil(bool Function() c, [String what = 'condition']) async {
    final end = DateTime.now().add(const Duration(seconds: 5));
    while (!c()) {
      if (DateTime.now().isAfter(end)) throw TimeoutException('Timed out: $what; statuses=$statuses');
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }
}

void main() {
  late FakeServer server;
  late CompatRecorder rec;
  late SignalR signalR;

  SignalR create({bool reportTransientErrors = false, Transport transport = Transport.auto}) => SignalR(
        server.baseUrl,
        'markethub',
        hubMethods: const <String>['PriceUpdate', 'Handshake', 'IndexUpdate'],
        queryString: 'session=s1',
        transport: transport,
        statusChangeCallback: rec.onStatus,
        hubCallback: rec.onMessage,
        connectionErrorCallback: rec.onError,
        reportTransientErrors: reportTransientErrors,
      );

  setUp(() async {
    server = FakeServer();
    await server.start();
    rec = CompatRecorder();
  });

  tearDown(() async {
    await signalR.stop();
    await server.close();
  });

  test('connect() returns at once; statuses are the plugin strings', () async {
    signalR = create();
    final id = await signalR.connect();
    expect(id, isNotNull);
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    expect(rec.statuses, <String>['ConnectionStatus.connecting', 'ConnectionStatus.connected']);
    expect(await signalR.isConnected(), isTrue);
    expect(signalR.connectionId, 'cid-1');
    expect(server.last('negotiate').query['session'], 's1');
    expect(server.last('negotiate').uri.path, '/app/signalr/negotiate');
  });

  test('hubCallback: registered methods only, first argument as text, non-strings JSON-encoded', () async {
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    server.push('MarketHub', 'priceupdate', <dynamic>['1=ABC,2=10.5', 'ignored']);
    server.push('MarketHub', 'NotRegistered', <dynamic>['x']);
    server.push('MarketHub', 'Handshake', <dynamic>[1]);
    server.push('MarketHub', 'IndexUpdate', <dynamic>[
      <String, dynamic>{'a': 1}
    ]);
    server.push('MarketHub', 'IndexUpdate', const <dynamic>[]);
    await rec.waitUntil(() => rec.messages.length == 4, '4 messages');
    expect(rec.messages, <List<String>>[
      <String>['PriceUpdate', '1=ABC,2=10.5'],
      <String>['Handshake', '1'],
      <String>['IndexUpdate', '{"a":1}'],
      <String>['IndexUpdate', ''],
    ]);
  });

  test('invokeMethod sends string arguments and returns the result as text', () async {
    server.onInvoke = (h, m, a, s) => Reply(result: m == 'Json' ? <String, dynamic>{'ok': true} : 'r:${a.first}');
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    expect(await signalR.invokeMethod('Handshake', arguments: <String>['user1']), 'r:user1');
    expect(await signalR.invokeMethod('Json'), '{"ok":true}');
    expect(server.invocations.first['A'], <dynamic>['user1']);
  });

  test('invokeMethod failure throws (the plugin hung forever on Android)', () async {
    server.onInvoke = (h, m, a, s) => Reply(error: 'bad request');
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    await expectLater(signalR.invokeMethod('X', arguments: <String>['y']), throwsA(anything));
  });

  test('invokeMethod before connect throws StateError', () async {
    signalR = create();
    expect(signalR.invokeMethod('X'), throwsA(isA<StateError>()));
  });

  test('start failure: connecting -> connectionError -> disconnected, with message', () async {
    server.negotiateStatus = 500;
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.disconnected'));
    expect(rec.statuses, <String>[
      'ConnectionStatus.connecting',
      'ConnectionStatus.connectionError',
      'ConnectionStatus.disconnected',
    ]);
    expect(rec.errors.single, contains('500'));
  });

  test('network drop: reconnecting -> connected, no connectionError by default', () async {
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    await server.dropTransport();
    await rec.waitUntil(() => rec.statuses.where((s) => s == 'ConnectionStatus.connected').length == 2, 'reconnected');
    expect(rec.statuses, <String>[
      'ConnectionStatus.connecting',
      'ConnectionStatus.connected',
      'ConnectionStatus.reconnecting',
      'ConnectionStatus.connected',
    ]);
    expect(rec.errors, isEmpty);
  });

  test('reportTransientErrors: a network drop also reports connectionError (plugin behaviour)', () async {
    signalR = create(reportTransientErrors: true);
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    await server.dropTransport();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connectionError'));
    expect(rec.errors, isNotEmpty);
  });

  test('connectionSlow is reported', () async {
    await server.close();
    server = FakeServer(keepAliveSeconds: 0.6, disconnectSeconds: 5);
    await server.start();
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connectionSlow'), 'slow');
  });

  test('stop -> disconnected; connect() again creates a fresh connection', () async {
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    await signalR.stop();
    await rec.waitUntil(() => rec.statuses.last == 'ConnectionStatus.disconnected');
    expect(await signalR.isConnected(), isFalse);
    await signalR.connect();
    await rec.waitUntil(
        () => rec.statuses.where((s) => s == 'ConnectionStatus.connected').length == 2, 'second connect');
    expect(server.of('negotiate'), hasLength(2));
  });

  test('connect() while connected is a no-op', () async {
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
    await signalR.connect();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.of('negotiate'), hasLength(1));
  });

  test('reconnect() after a failed start tries again', () async {
    server.negotiateStatus = 500;
    signalR = create();
    await signalR.connect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.disconnected'));
    server.negotiateStatus = 200;
    await signalR.reconnect();
    await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
  });

  for (final t in <Transport>[Transport.serverSentEvents, Transport.longPolling, Transport.webSockets]) {
    test('Transport.${t.name} is honoured', () async {
      signalR = create(transport: t);
      await signalR.connect();
      await rec.waitUntil(() => rec.statuses.contains('ConnectionStatus.connected'));
      expect(server.last('connect').query['transport'], t.name);
    });
  }
}
