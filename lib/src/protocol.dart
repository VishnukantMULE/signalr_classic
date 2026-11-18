import 'exceptions.dart';

/// Classic protocol versions this client understands.
/// 1.2 = SignalR 1.x, 1.3 = 2.0, 1.4 = 2.1 (adds /start), 1.5 = 2.2+.
const List<String> supportedProtocolVersions = <String>['1.2', '1.3', '1.4', '1.5'];

/// Compares dotted versions numerically ("1.10" > "1.9").
int compareProtocolVersions(String a, String b) {
  final pa = a.split('.');
  final pb = b.split('.');
  final n = pa.length > pb.length ? pa.length : pb.length;
  for (var i = 0; i < n; i++) {
    final x = i < pa.length ? int.tryParse(pa[i]) ?? 0 : 0;
    final y = i < pb.length ? int.tryParse(pb[i]) ?? 0 : 0;
    if (x != y) return x < y ? -1 : 1;
  }
  return 0;
}

Duration? _seconds(dynamic value) {
  if (value is num) return Duration(microseconds: (value * 1000000).round());
  if (value is String) {
    final v = num.tryParse(value);
    if (v != null) return Duration(microseconds: (v * 1000000).round());
  }
  return null;
}

/// Response of `GET /negotiate`.
class NegotiationResponse {
  NegotiationResponse({
    required this.connectionToken,
    required this.connectionId,
    required this.protocolVersion,
    required this.keepAliveTimeout,
    required this.disconnectTimeout,
    required this.connectionTimeout,
    required this.tryWebSockets,
    required this.transportConnectTimeout,
    required this.longPollDelay,
    this.url,
  });

  factory NegotiationResponse.fromJson(dynamic json) {
    if (json is! Map) throw NegotiationException('Negotiate response is not a JSON object');
    final token = json['ConnectionToken'];
    if (token is! String || token.isEmpty) {
      throw NegotiationException('Negotiate response has no ConnectionToken');
    }
    final protocol = json['ProtocolVersion'];
    if (protocol == null) throw NegotiationException('Negotiate response has no ProtocolVersion');
    return NegotiationResponse(
      connectionToken: token,
      connectionId: json['ConnectionId']?.toString(),
      protocolVersion: protocol.toString(),
      // null means keep-alive is disabled on the server.
      keepAliveTimeout: _seconds(json['KeepAliveTimeout']),
      disconnectTimeout: _seconds(json['DisconnectTimeout']) ?? const Duration(seconds: 30),
      connectionTimeout: _seconds(json['ConnectionTimeout']) ?? const Duration(seconds: 110),
      tryWebSockets: json['TryWebSockets'] != false,
      transportConnectTimeout: _seconds(json['TransportConnectTimeout']),
      longPollDelay: _seconds(json['LongPollDelay']) ?? Duration.zero,
      url: json['Url']?.toString(),
    );
  }

  final String connectionToken;
  final String? connectionId;
  final String protocolVersion;
  final Duration? keepAliveTimeout;
  final Duration disconnectTimeout;
  final Duration connectionTimeout;
  final bool tryWebSockets;
  final Duration? transportConnectTimeout;
  final Duration longPollDelay;
  final String? url;
}

/// What a transport needs to know after the connection processed a payload.
class ResponseInfo {
  const ResponseInfo({
    this.shouldReconnect = false,
    this.disconnect = false,
    this.longPollDelay,
    this.ignored = false,
  });

  static const ResponseInfo none = ResponseInfo();
  static const ResponseInfo stale = ResponseInfo(ignored: true);

  /// `T: 1`: the server asked the client to reconnect.
  final bool shouldReconnect;

  /// `D: 1`: the server ended the connection.
  final bool disconnect;

  /// `L`: delay before the next long poll.
  final Duration? longPollDelay;

  /// The payload came from a transport that is no longer current.
  final bool ignored;
}
