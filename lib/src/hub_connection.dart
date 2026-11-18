import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'exceptions.dart';
import 'http.dart';
import 'options.dart';
import 'protocol.dart';
import 'transports/long_polling_transport.dart';
import 'transports/server_sent_events_transport.dart';
import 'transports/transport.dart';
import 'transports/websocket_transport.dart';
import 'types.dart';

part 'hub_proxy.dart';

class _Superseded implements Exception {
  const _Superseded();
}

class _Pending {
  _Pending(this.hub, this.method, this.onProgress);

  final HubProxy hub;
  final String method;
  final ProgressHandler? onProgress;
  final Completer<dynamic> completer = Completer<dynamic>();
  Timer? timer;
}

/// Connection to a classic ASP.NET SignalR 2.x endpoint.
///
/// ```dart
/// final connection = HubConnection('https://host/app',
///     options: HubConnectionOptions(queryString: 'session=$session'));
/// final hub = connection.createHubProxy('markethub');
/// hub.on('PriceUpdate', (args) => print(args.first));
/// await connection.start();
/// await hub.invoke('PriceUpdate', [request]);
/// await connection.stop();
/// ```
class HubConnection implements TransportHost {
  HubConnection(this.url, {this.options = const HubConnectionOptions()})
      : _client = options.httpClientFactory?.call() ?? HttpClient(),
        _ownsClient = options.httpClientFactory == null {
    _http = SignalRHttp(_client, requestHeaders, options.httpTimeout);
  }

  /// Base URL. With `useDefaultPath` (default), `/signalr` is appended.
  final String url;
  final HubConnectionOptions options;

  final HttpClient _client;
  final bool _ownsClient;
  late final SignalRHttp _http;
  final Random _random = Random();

  final Map<String, HubProxy> _hubs = <String, HubProxy>{};

  ConnectionState _state = ConnectionState.disconnected;
  NegotiationResponse? _negotiation;
  String? _connectionToken;
  String? _protocol;
  String? _connectionId;
  String? _messageId;
  String? _groupsToken;
  Duration? _serverLongPollDelay;
  ClientTransport? _transport;

  int _generation = 0;
  Future<void>? _startFuture;
  Completer<void>? _initCompleter;

  final Map<String, _Pending> _pending = <String, _Pending>{};
  int _nextInvocationId = 0;

  Timer? _monitor;
  DateTime _lastMessageAt = DateTime.now();
  DateTime _lastActiveAt = DateTime.now();
  bool _slowRaised = false;
  bool _disposed = false;

  final StreamController<StateChange> _stateController = StreamController<StateChange>.broadcast();
  final StreamController<SignalRError> _errorController = StreamController<SignalRError>.broadcast();
  final StreamController<void> _slowController = StreamController<void>.broadcast();
  final StreamController<void> _reconnectingController = StreamController<void>.broadcast();
  final StreamController<void> _reconnectedController = StreamController<void>.broadcast();
  final StreamController<Object?> _closedController = StreamController<Object?>.broadcast();
  final StreamController<dynamic> _receivedController = StreamController<dynamic>.broadcast();

  // ------------------------------------------------------------------ public

  ConnectionState get state => _state;

  /// Server-assigned connection id (null while disconnected).
  String? get connectionId => _connectionId;

  /// Transport in use, or null.
  TransportType? get transport => _transport?.type;

  /// Protocol version agreed with the server, or null.
  String? get protocolVersion => _protocol;

  /// Cursor of the last received message batch (sent on reconnect).
  String? get messageId => _messageId;

  Stream<StateChange> get onStateChanged => _stateController.stream;

  /// Errors. Connection-level failures also close the connection.
  Stream<SignalRError> get onError => _errorController.stream;

  /// No keep-alive for 2/3 of KeepAliveTimeout.
  Stream<void> get onConnectionSlow => _slowController.stream;

  Stream<void> get onReconnecting => _reconnectingController.stream;
  Stream<void> get onReconnected => _reconnectedController.stream;

  /// Emits when the connection is disconnected; the value is the error that
  /// caused it, or null for a normal [stop].
  Stream<Object?> get onClosed => _closedController.stream;

  /// Non-hub messages (persistent-connection payloads).
  Stream<dynamic> get onReceived => _receivedController.stream;

  /// Creates (or returns) the proxy for [hubName]. Must be called before
  /// [start]: hub names are part of the connection data.
  HubProxy createHubProxy(String hubName) {
    final key = hubName.toLowerCase();
    final existing = _hubs[key];
    if (existing != null) return existing;
    if (_state != ConnectionState.disconnected) {
      throw InvalidStateException('Hub proxies must be created before the connection is started');
    }
    final proxy = HubProxy._(this, hubName);
    _hubs[key] = proxy;
    return proxy;
  }

  /// Starts the connection. Completes when connected; throws (and leaves the
  /// connection disconnected) on failure. Calling it while a start is in
  /// progress returns the same future.
  Future<void> start() {
    if (_disposed) return Future<void>.error(InvalidStateException('Connection has been disposed'));
    if (_state == ConnectionState.connected || _state == ConnectionState.reconnecting) {
      return Future<void>.value();
    }
    final running = _startFuture;
    if (running != null) return running;
    final generation = ++_generation;
    final future = _start(generation);
    _startFuture = future;
    future.then<void>(
      (_) => _clearStart(future),
      onError: (Object _) => _clearStart(future),
    );
    return future;
  }

  /// Stops the connection. Safe to call in any state.
  Future<void> stop() => _shutdown(null);

  /// Stops the connection, closes event streams and the owned HttpClient.
  Future<void> dispose() async {
    if (_disposed) return;
    await stop();
    _disposed = true;
    await Future.wait<void>(<Future<void>>[
      _stateController.close(),
      _errorController.close(),
      _slowController.close(),
      _reconnectingController.close(),
      _reconnectedController.close(),
      _closedController.close(),
      _receivedController.close(),
    ]);
    if (_ownsClient) _client.close(force: true);
  }

  // ------------------------------------------------------------------- start

  void _clearStart(Future<void> future) {
    if (identical(_startFuture, future)) _startFuture = null;
  }

  void _check(int generation) {
    if (generation != _generation) throw const _Superseded();
  }

  Future<void> _start(int generation) async {
    _setState(ConnectionState.connecting);
    try {
      final negotiation = await _negotiate();
      _check(generation);
      _negotiation = negotiation;
      _connectionToken = negotiation.connectionToken;
      _connectionId = negotiation.connectionId;
      _protocol = _resolveProtocol(negotiation.protocolVersion);
      _serverLongPollDelay = null;
      _messageId = null;
      _groupsToken = null;

      Object? lastError;
      ClientTransport? started;
      for (final type in _candidates(negotiation)) {
        _check(generation);
        final transport = _createTransport(type);
        _transport = transport;
        final init = Completer<void>();
        unawaited(init.future.catchError((Object _) {}));
        _initCompleter = init;
        try {
          await transport.start(reconnect: false);
          _check(generation);
          _ensureAlive(transport);
          if (_waitsForInitMessage && !init.isCompleted) {
            await init.future.timeout(transportConnectTimeout, onTimeout: () {
              throw TransportException('No init message within ${transportConnectTimeout.inMilliseconds} ms');
            });
          }
          _check(generation);
          started = transport;
          break;
        } catch (e) {
          if (identical(_transport, transport)) _transport = null;
          await transport.close();
          if (e is _Superseded) rethrow;
          _check(generation);
          lastError = e;
          log(LogLevel.warning, () => 'Transport ${type.name} failed: $e');
          if (e is HttpStatusException && e.isAuthError) rethrow;
          if (options.transport != TransportType.auto) rethrow;
        } finally {
          if (identical(_initCompleter, init)) _initCompleter = null;
        }
      }
      if (started == null) {
        throw TransportException(
            'No transport could be initialized successfully. Try specifying a different transport.', lastError);
      }

      if (compareProtocolVersions(_protocol!, '1.4') >= 0) {
        final body = await _http.get(_actionUri('start', started.type));
        _check(generation);
        final json = _tryDecode(body);
        if (json is! Map || json['Response'] != 'started') {
          throw NegotiationException('Unexpected response from /start');
        }
      }
      _ensureAlive(started);

      _lastMessageAt = DateTime.now();
      _lastActiveAt = _lastMessageAt;
      _slowRaised = false;
      _setState(ConnectionState.connected);
      log(LogLevel.information, () => 'Connected (transport: ${started!.type.name}, protocol: $_protocol)');
      _startMonitor(generation);
    } catch (e, st) {
      if (e is _Superseded || generation != _generation) {
        // stop() already moved to disconnected and reported it.
        throw InvalidStateException('The connection was stopped while starting');
      }
      await _shutdown(e, st);
      rethrow;
    }
  }

  bool get _waitsForInitMessage => compareProtocolVersions(_protocol ?? '1.5', '1.3') >= 0;

  Future<NegotiationResponse> _negotiate() async {
    final uri = _buildUri('negotiate', <String, String?>{
      'clientProtocol': options.clientProtocol,
      'connectionData': _connectionData,
      '_': DateTime.now().millisecondsSinceEpoch.toString(),
    });
    log(LogLevel.debug, () => 'Negotiating with ${uri.path}');
    final body = await _http.get(uri);
    final json = _tryDecode(body);
    if (json == null) throw NegotiationException('Negotiate response is not valid JSON');
    return NegotiationResponse.fromJson(json);
  }

  String _resolveProtocol(String server) {
    if (server == options.clientProtocol) return server;
    if (supportedProtocolVersions.contains(server)) {
      log(LogLevel.information, () => 'Server uses protocol $server (requested ${options.clientProtocol}); adapting');
      return server;
    }
    throw UnsupportedProtocolException(server, supportedProtocolVersions);
  }

  List<TransportType> _candidates(NegotiationResponse negotiation) {
    switch (options.transport) {
      case TransportType.webSockets:
        return const <TransportType>[TransportType.webSockets];
      case TransportType.serverSentEvents:
        return const <TransportType>[TransportType.serverSentEvents];
      case TransportType.longPolling:
        return const <TransportType>[TransportType.longPolling];
      case TransportType.auto:
        return <TransportType>[
          if (negotiation.tryWebSockets) TransportType.webSockets,
          TransportType.serverSentEvents,
          TransportType.longPolling,
        ];
    }
  }

  ClientTransport _createTransport(TransportType type) {
    switch (type) {
      case TransportType.webSockets:
        return WebSocketTransport(this);
      case TransportType.serverSentEvents:
        return ServerSentEventsTransport(this);
      case TransportType.longPolling:
      case TransportType.auto:
        return LongPollingTransport(this);
    }
  }

  // -------------------------------------------------------------- shutdown

  Future<void> _shutdown(Object? error, [StackTrace? stackTrace]) async {
    if (_state == ConnectionState.disconnected) return;
    _generation++;
    _stopMonitor();
    final transport = _transport;
    _transport = null;
    final init = _initCompleter;
    _initCompleter = null;
    if (init != null && !init.isCompleted) init.completeError(const _Superseded());

    if (transport != null && _connectionToken != null) {
      final abortUri = _actionUri('abort', transport.type);
      unawaited(
          _http.post(abortUri, '', timeout: const Duration(seconds: 5)).then<void>((_) {}, onError: (Object _) {}));
    }
    await transport?.close();

    _failPending(ConnectionClosedException('Connection was stopped before the invocation completed'));
    _connectionToken = null;
    _connectionId = null;
    _messageId = null;
    _groupsToken = null;

    if (error != null) _emitError(error, ErrorKind.connection, stackTrace);
    _setState(ConnectionState.disconnected);
    if (!_closedController.isClosed) _closedController.add(error);
    log(LogLevel.information, () => error == null ? 'Disconnected' : 'Disconnected: $error');
  }

  // --------------------------------------------------------------- reconnect

  Duration get _reconnectWindow {
    final configured = options.reconnectWindow;
    if (configured != null) return configured;
    final negotiation = _negotiation;
    if (negotiation == null) return const Duration(seconds: 30);
    final keepAlive = negotiation.keepAliveTimeout;
    return keepAlive == null ? negotiation.disconnectTimeout : keepAlive + negotiation.disconnectTimeout;
  }

  void _beginReconnect(ClientTransport lost, Object error) {
    if (_state != ConnectionState.connected) return;
    _stopMonitor();
    if (DateTime.now().difference(_lastActiveAt) > _reconnectWindow) {
      unawaited(_shutdown(SignalRException(
          'The client was inactive for longer than the reconnect window; the server has dropped the connection')));
      return;
    }
    final generation = ++_generation;
    _transport = null;
    unawaited(lost.close());
    _failPending(ConnectionClosedException('Connection was lost before the invocation completed', error));
    _emitError(error, ErrorKind.transport);
    _setState(ConnectionState.reconnecting);
    if (!_reconnectingController.isClosed) _reconnectingController.add(null);
    log(LogLevel.warning, () => 'Connection lost ($error); reconnecting');
    unawaited(_reconnectLoop(generation, lost.type));
  }

  Future<void> _reconnectLoop(int generation, TransportType type) async {
    while (generation == _generation) {
      if (DateTime.now().difference(_lastActiveAt) > _reconnectWindow) {
        await _shutdown(SignalRException('Could not reconnect within ${_reconnectWindow.inMilliseconds} ms'));
        return;
      }
      final transport = _createTransport(type);
      _transport = transport;
      try {
        await transport.start(reconnect: true);
        if (generation != _generation) {
          await transport.close();
          return;
        }
        _ensureAlive(transport);
        _lastMessageAt = DateTime.now();
        _lastActiveAt = _lastMessageAt;
        _slowRaised = false;
        _setState(ConnectionState.connected);
        if (!_reconnectedController.isClosed) _reconnectedController.add(null);
        log(LogLevel.information, () => 'Reconnected (transport: ${type.name})');
        _startMonitor(generation);
        return;
      } catch (e) {
        if (identical(_transport, transport)) _transport = null;
        await transport.close();
        if (generation != _generation) return;
        if (e is HttpStatusException && e.isAuthError) {
          await _shutdown(e);
          return;
        }
        log(LogLevel.debug, () => 'Reconnect attempt failed: $e');
        await Future<void>.delayed(options.reconnectDelay);
      }
    }
  }

  // ------------------------------------------------------------- keep-alive

  void _startMonitor(int generation) {
    _stopMonitor();
    final keepAlive = _negotiation?.keepAliveTimeout;
    final usesKeepAlive = keepAlive != null && (_transport?.supportsKeepAlive ?? false);
    Duration interval;
    if (usesKeepAlive) {
      final warnAt = keepAlive * 2 ~/ 3;
      interval = (keepAlive - warnAt) ~/ 3;
    } else {
      interval = const Duration(seconds: 5);
    }
    if (interval < const Duration(milliseconds: 50)) interval = const Duration(milliseconds: 50);
    _monitor = Timer.periodic(interval, (_) => _beat(generation, usesKeepAlive ? keepAlive : null));
  }

  void _stopMonitor() {
    _monitor?.cancel();
    _monitor = null;
  }

  void _beat(int generation, Duration? keepAlive) {
    if (generation != _generation || _state != ConnectionState.connected) return;
    final transport = _transport;
    if (transport == null) return;
    final now = DateTime.now();
    if (keepAlive == null) return; // Long polling or keep-alive disabled.

    // Timers do not run while the app is suspended. If we were away longer
    // than the server keeps a connection, reconnecting cannot succeed.
    if (now.difference(_lastActiveAt) > _reconnectWindow) {
      unawaited(_shutdown(SignalRException(
          'The client was inactive for longer than the reconnect window; the server has dropped the connection')));
      return;
    }
    final silence = now.difference(_lastMessageAt);
    if (silence >= keepAlive) {
      onTransportLost(transport, TransportException('No keep-alive received for ${silence.inMilliseconds} ms'));
    } else if (silence >= keepAlive * 2 ~/ 3 && !_slowRaised) {
      _slowRaised = true;
      log(LogLevel.warning, () => 'Connection slow: no data for ${silence.inMilliseconds} ms');
      if (!_slowController.isClosed) _slowController.add(null);
    }
  }

  // ------------------------------------------------------------- invocation

  Future<dynamic> _invoke(
    HubProxy hub,
    String method,
    List<dynamic> arguments,
    ProgressHandler? onProgress,
    Duration? timeout,
  ) {
    final transport = _transport;
    if (_state != ConnectionState.connected || transport == null) {
      return Future<dynamic>.error(InvalidStateException('Cannot invoke "$method": the connection is ${_state.name}'));
    }
    final id = _nextInvocationId++;
    final message = <String, dynamic>{
      'H': hub.hubName,
      'M': method,
      'A': arguments,
      'I': id,
    };
    if (hub.state.isNotEmpty) message['S'] = hub.state;
    String payload;
    try {
      payload = jsonEncode(message);
    } catch (e) {
      return Future<dynamic>.error(ArgumentError('Arguments for "$method" are not JSON-encodable: $e'));
    }
    final key = id.toString();
    final pending = _Pending(hub, method, onProgress);
    _pending[key] = pending;
    final limit = timeout ?? options.invocationTimeout;
    if (limit != null) {
      pending.timer = Timer(limit, () {
        if (identical(_pending[key], pending)) {
          _pending.remove(key);
          pending.completer.completeError(InvocationTimeoutException(method, limit));
        }
      });
    }
    if (options.logMessageContent) log(LogLevel.trace, () => 'Send: $payload');
    transport.send(payload).then<void>((_) {}, onError: (Object e) {
      if (identical(_pending[key], pending)) {
        _pending.remove(key);
        pending.timer?.cancel();
        pending.completer.completeError(TransportException('Failed to send invocation "$method"', e));
      }
    });
    return pending.completer.future;
  }

  void _failPending(Object error) {
    if (_pending.isEmpty) return;
    final pending = List<_Pending>.from(_pending.values);
    _pending.clear();
    for (final p in pending) {
      p.timer?.cancel();
      if (!p.completer.isCompleted) p.completer.completeError(error);
    }
  }

  // ---------------------------------------------------------------- messages

  @override
  ResponseInfo onTransportData(ClientTransport transport, String data) {
    if (!identical(transport, _transport)) return ResponseInfo.stale;
    final now = DateTime.now();
    _lastMessageAt = now;
    _lastActiveAt = now;
    _slowRaised = false;

    final text = data.trim();
    if (text.isEmpty) return ResponseInfo.none;
    if (options.logMessageContent) log(LogLevel.trace, () => 'Received: $text');
    final json = _tryDecode(text);
    if (json == null) {
      _emitError(SignalRException('Received a message that is not valid JSON'), ErrorKind.protocol);
      return ResponseInfo.none;
    }
    if (json is! Map) {
      if (!_receivedController.isClosed) _receivedController.add(json);
      return ResponseInfo.none;
    }
    if (json.isEmpty) return ResponseInfo.none; // keep-alive

    if (json.containsKey('I')) {
      _onInvocationResult(json);
      return ResponseInfo.none;
    }

    if (_isOne(json['D'])) {
      log(LogLevel.information, () => 'Server requested disconnect');
      scheduleMicrotask(() {
        if (identical(transport, _transport)) {
          unawaited(_shutdown(SignalRException('The server closed the connection')));
        }
      });
      return const ResponseInfo(disconnect: true);
    }

    final groups = json['G'];
    if (groups != null) _groupsToken = groups.toString();
    final cursor = json['C'];
    if (cursor != null) _messageId = cursor.toString();

    final messages = json['M'];
    if (messages is List) {
      for (final message in messages) {
        if (message is Map && message.containsKey('H')) {
          _dispatchHubMessage(message);
        } else if (!_receivedController.isClosed) {
          _receivedController.add(message);
        }
      }
    }

    if (_isOne(json['S'])) {
      final init = _initCompleter;
      if (init != null && !init.isCompleted) init.complete();
    }

    Duration? longPollDelay;
    final l = json['L'];
    if (l is num) {
      longPollDelay = Duration(milliseconds: l.round());
      _serverLongPollDelay = longPollDelay;
    }

    final shouldReconnect = _isOne(json['T']);
    if (shouldReconnect && transport.type != TransportType.longPolling) {
      scheduleMicrotask(() => onTransportLost(transport, TransportException('Server requested a reconnect')));
    }
    return ResponseInfo(shouldReconnect: shouldReconnect, longPollDelay: longPollDelay);
  }

  void _onInvocationResult(Map<dynamic, dynamic> json) {
    final id = json['I'].toString();
    if (id.startsWith('P|')) {
      final progress = json['P'];
      if (progress is Map) {
        final pending = _pending[progress['I']?.toString()];
        final handler = pending?.onProgress;
        if (handler != null) {
          try {
            handler(progress['D']);
          } catch (e, st) {
            _emitError(e, ErrorKind.handler, st);
          }
        }
      }
      return;
    }
    final pending = _pending.remove(id);
    if (pending == null) return;
    pending.timer?.cancel();
    final hubState = json['S'];
    if (hubState is Map) {
      hubState.forEach((dynamic k, dynamic v) => pending.hub.state[k.toString()] = v);
    }
    final error = json['E'];
    if (error != null) {
      final trace = json['T'];
      pending.completer.completeError(HubException(
        error.toString(),
        isHubException: json['H'] == true,
        errorData: json['D'],
        serverStackTrace: trace?.toString(),
      ));
    } else {
      pending.completer.complete(json['R']);
    }
  }

  void _dispatchHubMessage(Map<dynamic, dynamic> message) {
    final hub = _hubs[message['H'].toString().toLowerCase()];
    if (hub == null) return;
    final hubState = message['S'];
    if (hubState is Map) {
      hubState.forEach((dynamic k, dynamic v) => hub.state[k.toString()] = v);
    }
    final method = message['M']?.toString();
    if (method == null) return;
    final args = message['A'];
    hub._dispatch(method, args is List ? List<dynamic>.from(args) : <dynamic>[]);
  }

  @override
  void onTransportLost(ClientTransport transport, Object error) {
    if (!identical(transport, _transport)) return;
    if (_state != ConnectionState.connected) {
      // Still starting or reconnecting: the start/reconnect code checks this
      // flag before declaring the connection up.
      _lostBeforeReady = transport;
      final init = _initCompleter;
      if (init != null && !init.isCompleted) init.completeError(error);
      return;
    }
    _beginReconnect(transport, error);
  }

  ClientTransport? _lostBeforeReady;

  void _ensureAlive(ClientTransport transport) {
    if (identical(_lostBeforeReady, transport)) {
      throw TransportException('Transport closed before the connection was established');
    }
  }

  // ------------------------------------------------------- TransportHost impl

  @override
  SignalRHttp get http => _http;

  @override
  HttpClient get httpClient => _client;

  @override
  Map<String, String> get requestHeaders => options.headers ?? const <String, String>{};

  @override
  Duration get transportConnectTimeout {
    final configured = options.transportConnectTimeout;
    if (configured != null) return configured;
    final server = _negotiation?.transportConnectTimeout;
    const floor = Duration(seconds: 5);
    if (server == null || server < floor) return floor;
    return server;
  }

  @override
  Duration get pollTimeout =>
      (_negotiation?.connectionTimeout ?? const Duration(seconds: 110)) + const Duration(seconds: 10);

  @override
  Duration get longPollDelay => _serverLongPollDelay ?? _negotiation?.longPollDelay ?? Duration.zero;

  @override
  Duration get reconnectDelay => options.reconnectDelay;

  @override
  void markActive() => _lastActiveAt = DateTime.now();

  @override
  Uri connectUri(TransportType type, {required bool reconnect}) => _buildUri(
        reconnect ? 'reconnect' : 'connect',
        <String, String?>{
          'transport': type.name,
          'clientProtocol': _protocol ?? options.clientProtocol,
          'connectionToken': _connectionToken,
          'connectionData': _connectionData,
          if (reconnect) 'messageId': _messageId,
          if (reconnect) 'groupsToken': _groupsToken,
        },
        tid: true,
      );

  @override
  Uri pollUri({required bool reconnect}) => _buildUri(
        reconnect ? 'reconnect' : 'poll',
        <String, String?>{
          'transport': TransportType.longPolling.name,
          'clientProtocol': _protocol ?? options.clientProtocol,
          'connectionToken': _connectionToken,
          'connectionData': _connectionData,
          'messageId': _messageId,
          'groupsToken': _groupsToken,
        },
        tid: true,
      );

  @override
  Uri sendUri(TransportType type) => _actionUri('send', type);

  Uri _actionUri(String action, TransportType type) => _buildUri(action, <String, String?>{
        'transport': type.name,
        'clientProtocol': _protocol ?? options.clientProtocol,
        'connectionToken': _connectionToken,
        'connectionData': _connectionData,
      });

  @override
  void log(LogLevel level, String Function() message) {
    final logger = options.logger;
    if (logger == null) return;
    try {
      logger(level, message());
    } catch (_) {
      // A failing logger must never break the connection.
    }
  }

  // ----------------------------------------------------------------- helpers

  String get _endpoint {
    var base = url.trim();
    while (base.endsWith('/')) {
      base = base.substring(0, base.length - 1);
    }
    if (options.useDefaultPath) base = '$base/signalr';
    return '$base/';
  }

  String get _connectionData => jsonEncode(<Map<String, String>>[
        for (final hub in _hubs.values) <String, String>{'name': hub.hubName.toLowerCase()},
      ]);

  Uri _buildUri(String action, Map<String, String?> params, {bool tid = false}) {
    final sb = StringBuffer(_endpoint)..write(action);
    var first = true;
    void add(String raw) {
      sb.write(first ? '?' : '&');
      sb.write(raw);
      first = false;
    }

    params.forEach((key, value) {
      if (value == null) return;
      add('${Uri.encodeQueryComponent(key)}=${Uri.encodeQueryComponent(value)}');
    });
    final raw = options.queryString?.trim();
    if (raw != null && raw.isNotEmpty) {
      add(raw.replaceFirst(RegExp(r'^[?&]+'), ''));
    }
    options.queryParameters?.forEach((key, value) {
      add('${Uri.encodeQueryComponent(key)}=${Uri.encodeQueryComponent(value)}');
    });
    if (tid) add('tid=${_random.nextInt(11)}');
    return Uri.parse(sb.toString());
  }

  void _setState(ConnectionState next) {
    if (_state == next) return;
    final previous = _state;
    _state = next;
    if (!_stateController.isClosed) _stateController.add(StateChange(previous, next));
  }

  void _emitError(Object error, ErrorKind kind, [StackTrace? stackTrace]) {
    log(kind == ErrorKind.handler ? LogLevel.warning : LogLevel.error, () => '${kind.name} error: $error');
    if (!_errorController.isClosed) _errorController.add(SignalRError(error, kind, stackTrace));
  }

  static bool _isOne(dynamic value) => value == 1 || value == true || value == '1';

  static dynamic _tryDecode(String text) {
    try {
      return jsonDecode(text);
    } catch (_) {
      return null;
    }
  }
}
