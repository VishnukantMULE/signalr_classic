/// Lifecycle states, matching the official classic clients.
enum ConnectionState { connecting, connected, reconnecting, disconnected }

/// Classic SignalR transports. [name] is the value sent on the wire.
enum TransportType {
  /// WebSockets when the server allows it, then Server-Sent Events, then
  /// Long Polling (the order used by the official JavaScript client).
  auto,
  webSockets,
  serverSentEvents,
  longPolling,
}

enum LogLevel { trace, debug, information, warning, error }

/// Receives log lines. Messages never contain connection tokens or headers;
/// payloads are only included when `logMessageContent` is enabled.
typedef SignalRLogger = void Function(LogLevel level, String message);

/// Handler for a client method invoked by the server. [arguments] are the
/// decoded JSON values sent by the server.
typedef HubMethodHandler = void Function(List<dynamic> arguments);

/// Called with progress data for an invocation that reports progress.
typedef ProgressHandler = void Function(dynamic data);

class StateChange {
  const StateChange(this.previous, this.current);

  final ConnectionState previous;
  final ConnectionState current;

  @override
  String toString() => 'StateChange(${previous.name} -> ${current.name})';
}

/// What an error event relates to.
enum ErrorKind {
  /// The connection failed or was closed because of this error
  /// (start failure, auth rejection, reconnect window exceeded, ...).
  connection,

  /// A transport dropped; the connection is trying to reconnect.
  transport,

  /// A client method handler threw.
  handler,

  /// A server message could not be parsed.
  protocol,
}

class SignalRError {
  const SignalRError(this.error, this.kind, [this.stackTrace]);

  final Object error;
  final ErrorKind kind;
  final StackTrace? stackTrace;

  @override
  String toString() => 'SignalRError(${kind.name}): $error';
}
