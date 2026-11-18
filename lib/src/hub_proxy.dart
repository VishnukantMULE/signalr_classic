part of 'hub_connection.dart';

/// Client-side proxy for one server hub. Create it with
/// [HubConnection.createHubProxy] before starting the connection.
class HubProxy {
  HubProxy._(this._connection, this.hubName);

  final HubConnection _connection;

  /// Hub name as given (sent in invocations; matched case-insensitively).
  final String hubName;

  /// Hub state (`S`): sent with invocations and updated from the server.
  final Map<String, dynamic> state = <String, dynamic>{};

  final Map<String, List<HubMethodHandler>> _handlers = <String, List<HubMethodHandler>>{};

  /// Registers [handler] for client method [method] (case-insensitive).
  /// Several handlers per method are allowed; they run in registration order.
  void on(String method, HubMethodHandler handler) {
    _handlers.putIfAbsent(method.toLowerCase(), () => <HubMethodHandler>[]).add(handler);
  }

  /// Removes [handler], or every handler of [method] when [handler] is null.
  void off(String method, [HubMethodHandler? handler]) {
    final key = method.toLowerCase();
    if (handler == null) {
      _handlers.remove(key);
      return;
    }
    final list = _handlers[key];
    if (list == null) return;
    list.remove(handler);
    if (list.isEmpty) _handlers.remove(key);
  }

  /// Calls server method [method]. Completes with the decoded result (`R`),
  /// or fails with [HubException], [ConnectionClosedException],
  /// [InvocationTimeoutException], [TransportException] or
  /// [InvalidStateException] (when not connected).
  Future<dynamic> invoke(
    String method, [
    List<dynamic> arguments = const <dynamic>[],
    ProgressHandler? onProgress,
    Duration? timeout,
  ]) =>
      _connection._invoke(this, method, arguments, onProgress, timeout);

  void _dispatch(String method, List<dynamic> arguments) {
    final list = _handlers[method.toLowerCase()];
    if (list == null || list.isEmpty) return;
    for (final handler in List<HubMethodHandler>.from(list)) {
      try {
        handler(arguments);
      } catch (e, st) {
        _connection._emitError(e, ErrorKind.handler, st);
      }
    }
  }
}
