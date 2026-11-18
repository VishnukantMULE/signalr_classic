class SignalRException implements Exception {
  SignalRException(this.message, [this.cause]);

  final String message;
  final Object? cause;

  @override
  String toString() => cause == null ? '$runtimeType: $message' : '$runtimeType: $message (cause: $cause)';
}

/// A non-success HTTP status from the server. [path] never includes the query
/// string, so connection tokens are not leaked into logs.
class HttpStatusException extends SignalRException {
  HttpStatusException(this.statusCode, this.path) : super('HTTP $statusCode from $path');

  final int statusCode;
  final String path;

  /// 401/403: retrying will not help.
  bool get isAuthError => statusCode == 401 || statusCode == 403;
}

class NegotiationException extends SignalRException {
  NegotiationException(super.message, [super.cause]);
}

class UnsupportedProtocolException extends SignalRException {
  UnsupportedProtocolException(this.serverVersion, List<String> supported)
      : super('Server protocol version $serverVersion is not supported '
            '(supported: ${supported.join(', ')}).');

  final String serverVersion;
}

class TransportException extends SignalRException {
  TransportException(super.message, [super.cause]);
}

class InvalidStateException extends SignalRException {
  InvalidStateException(super.message, [super.cause]);
}

/// The connection stopped or dropped before an invocation result arrived.
class ConnectionClosedException extends SignalRException {
  ConnectionClosedException(super.message, [super.cause]);
}

class InvocationTimeoutException extends SignalRException {
  InvocationTimeoutException(String method, Duration timeout)
      : super('Invocation of "$method" timed out after ${timeout.inMilliseconds} ms');
}

/// The hub method failed on the server (`E` in the invocation result).
class HubException extends SignalRException {
  HubException(
    super.message, {
    this.isHubException = false,
    this.errorData,
    this.serverStackTrace,
  });

  /// True when the server threw a `HubException` on purpose (`H`).
  final bool isHubException;

  /// Extra data attached by the server (`D`).
  final dynamic errorData;

  /// Server stack trace, when detailed errors are enabled on the server (`T`).
  final String? serverStackTrace;
}
