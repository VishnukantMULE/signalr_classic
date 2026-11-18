import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../hub_connection.dart';
import '../options.dart';
import '../types.dart';

/// Same values (and `toString()`) as signalr_flutter's `ConnectionStatus`,
/// so code comparing `'$status'` strings keeps working.
enum ConnectionStatus {
  connecting,
  connected,
  reconnecting,
  disconnected,
  connectionSlow,
  connectionError,
}

/// signalr_flutter's `Transport`, plus [webSockets].
enum Transport { auto, serverSentEvents, longPolling, webSockets }

/// Drop-in replacement for `package:signalr_flutter`'s `SignalR` class.
/// Switching an app over only needs the import changed to
/// `package:signalr_classic/signalr_flutter_compat.dart`.
///
/// Differences from the plugin, all intentional:
/// * One implementation on Android and iOS (no Java client, no hidden WebView).
/// * [Transport.auto] tries WebSockets first on both platforms.
/// * [invokeMethod] always completes: on failure it throws instead of hanging.
/// * A short network drop reports `reconnecting` and recovers by itself.
///   `connectionError` is reported only when the connection fails or closes
///   because of an error. Set [reportTransientErrors] to also report it for
///   each dropped transport, like the plugin did.
class SignalR {
  SignalR(
    this.baseUrl,
    this.hubName, {
    this.queryString,
    this.headers,
    this.hubMethods,
    this.transport = Transport.auto,
    this.statusChangeCallback,
    this.hubCallback,
    this.connectionErrorCallback,
    this.reportTransientErrors = false,
    this.httpClientFactory,
    this.logger,
    this.invocationTimeout,
  });

  final String baseUrl;
  final String hubName;
  final String? queryString;
  final Map<String, String>? headers;
  final List<String>? hubMethods;
  final Transport transport;

  /// Receives [ConnectionStatus] values.
  final void Function(dynamic status)? statusChangeCallback;

  /// `(methodName, message)`: [methodName] as listed in [hubMethods];
  /// [message] is the first argument, as text (non-strings are JSON-encoded).
  final void Function(String methodName, String message)? hubCallback;

  final void Function(String? error)? connectionErrorCallback;
  final bool reportTransientErrors;
  final HttpClient Function()? httpClientFactory;
  final SignalRLogger? logger;
  final Duration? invocationTimeout;

  /// Connection id assigned by the server.
  String? connectionId;

  HubConnection? _connection;
  HubProxy? _hub;
  final List<StreamSubscription<dynamic>> _subscriptions = <StreamSubscription<dynamic>>[];

  /// Starts connecting and returns at once (like the plugin); progress is
  /// reported through [statusChangeCallback]. Returns the connection id if
  /// already connected, otherwise ''.
  Future<String?> connect() async {
    final existing = _connection;
    if (existing != null && existing.state != ConnectionState.disconnected) {
      return connectionId ?? '';
    }
    await _disposeConnection();
    final connection = HubConnection(
      baseUrl,
      options: HubConnectionOptions(
        queryString: queryString,
        headers: headers,
        transport: _mapTransport(transport),
        httpClientFactory: httpClientFactory,
        logger: logger,
        invocationTimeout: invocationTimeout,
      ),
    );
    final hub = connection.createHubProxy(hubName);
    for (final method in hubMethods ?? const <String>[]) {
      hub.on(method, (args) => hubCallback?.call(method, _firstArgument(args)));
    }
    _connection = connection;
    _hub = hub;
    _listen(connection);
    _start(connection);
    return connectionId ?? '';
  }

  /// Like the plugin: starts the existing connection again.
  Future<String?> reconnect() async {
    final connection = _connection;
    if (connection == null) return connect();
    if (connection.state == ConnectionState.disconnected) _start(connection);
    return connectionId ?? '';
  }

  Future<void> stop() async {
    await _connection?.stop();
  }

  Future<bool> isConnected() async => _connection?.state == ConnectionState.connected;

  /// Invokes [methodName]. Completes with the result as text ('' for null);
  /// throws on failure.
  Future<String> invokeMethod(String methodName, {List<String>? arguments}) async {
    final hub = _hub;
    if (hub == null) {
      throw StateError('SignalR connection not started; call connect() first');
    }
    final result = await hub.invoke(methodName, arguments ?? const <String>[]);
    return _asText(result);
  }

  // ------------------------------------------------------------------ private

  void _start(HubConnection connection) {
    connection.start().then<void>((_) {
      connectionId = connection.connectionId;
    }, onError: (Object _) {
      // Reported through the error and status streams.
    });
  }

  void _listen(HubConnection connection) {
    _subscriptions.add(connection.onStateChanged.listen((change) {
      switch (change.current) {
        case ConnectionState.connecting:
          _status(ConnectionStatus.connecting);
          break;
        case ConnectionState.connected:
          connectionId = connection.connectionId;
          _status(ConnectionStatus.connected);
          break;
        case ConnectionState.reconnecting:
          _status(ConnectionStatus.reconnecting);
          break;
        case ConnectionState.disconnected:
          _status(ConnectionStatus.disconnected);
          break;
      }
    }));
    _subscriptions.add(connection.onConnectionSlow.listen((_) => _status(ConnectionStatus.connectionSlow)));
    _subscriptions.add(connection.onError.listen((e) {
      final report = e.kind == ErrorKind.connection || (reportTransientErrors && e.kind == ErrorKind.transport);
      if (!report) return;
      _status(ConnectionStatus.connectionError);
      connectionErrorCallback?.call(e.error.toString());
    }));
  }

  void _status(ConnectionStatus status) => statusChangeCallback?.call(status);

  Future<void> _disposeConnection() async {
    for (final s in _subscriptions) {
      await s.cancel();
    }
    _subscriptions.clear();
    final old = _connection;
    _connection = null;
    _hub = null;
    if (old != null) await old.dispose();
  }

  static TransportType _mapTransport(Transport t) {
    switch (t) {
      case Transport.auto:
        return TransportType.auto;
      case Transport.serverSentEvents:
        return TransportType.serverSentEvents;
      case Transport.longPolling:
        return TransportType.longPolling;
      case Transport.webSockets:
        return TransportType.webSockets;
    }
  }

  static String _firstArgument(List<dynamic> args) => args.isEmpty ? '' : _asText(args.first);

  static String _asText(dynamic value) {
    if (value == null) return '';
    if (value is String) return value;
    return jsonEncode(value);
  }
}
