import 'package:signalr_classic/signalr_classic.dart';
import 'package:signalr_classic/src/protocol.dart';
import 'package:test/test.dart';

void main() {
  test('compareProtocolVersions is numeric', () {
    expect(compareProtocolVersions('1.5', '1.4'), 1);
    expect(compareProtocolVersions('1.4', '1.4'), 0);
    expect(compareProtocolVersions('1.3', '1.10'), -1);
    expect(compareProtocolVersions('1', '1.0'), 0);
  });

  test('supported versions', () {
    expect(supportedProtocolVersions, <String>['1.2', '1.3', '1.4', '1.5']);
  });

  group('NegotiationResponse', () {
    test('parses a full response', () {
      final r = NegotiationResponse.fromJson(<String, dynamic>{
        'Url': '/signalr',
        'ConnectionToken': 'tok',
        'ConnectionId': 'id',
        'KeepAliveTimeout': 20.0,
        'DisconnectTimeout': 30.0,
        'ConnectionTimeout': 110.0,
        'TryWebSockets': true,
        'ProtocolVersion': '1.5',
        'TransportConnectTimeout': 5.0,
        'LongPollDelay': 0.0,
      });
      expect(r.connectionToken, 'tok');
      expect(r.connectionId, 'id');
      expect(r.keepAliveTimeout, const Duration(seconds: 20));
      expect(r.disconnectTimeout, const Duration(seconds: 30));
      expect(r.connectionTimeout, const Duration(seconds: 110));
      expect(r.tryWebSockets, isTrue);
      expect(r.protocolVersion, '1.5');
      expect(r.transportConnectTimeout, const Duration(seconds: 5));
    });

    test('null KeepAliveTimeout means disabled; defaults for missing fields', () {
      final r = NegotiationResponse.fromJson(<String, dynamic>{
        'ConnectionToken': 'tok',
        'ProtocolVersion': '1.3',
        'KeepAliveTimeout': null,
      });
      expect(r.keepAliveTimeout, isNull);
      expect(r.disconnectTimeout, const Duration(seconds: 30));
      expect(r.tryWebSockets, isTrue);
      expect(r.longPollDelay, Duration.zero);
    });

    test('fractional and string seconds', () {
      final r = NegotiationResponse.fromJson(<String, dynamic>{
        'ConnectionToken': 'tok',
        'ProtocolVersion': '1.5',
        'KeepAliveTimeout': '0.6',
        'DisconnectTimeout': 1.25,
      });
      expect(r.keepAliveTimeout, const Duration(milliseconds: 600));
      expect(r.disconnectTimeout, const Duration(milliseconds: 1250));
    });

    test('rejects missing token, missing protocol, non-object', () {
      expect(() => NegotiationResponse.fromJson(<String, dynamic>{'ProtocolVersion': '1.5'}),
          throwsA(isA<NegotiationException>()));
      expect(() => NegotiationResponse.fromJson(<String, dynamic>{'ConnectionToken': 't'}),
          throwsA(isA<NegotiationException>()));
      expect(() => NegotiationResponse.fromJson(<dynamic>[]), throwsA(isA<NegotiationException>()));
    });
  });

  test('HttpStatusException hides the query string and flags auth errors', () {
    final e = HttpStatusException(403, '/app/signalr/reconnect');
    expect(e.isAuthError, isTrue);
    expect(e.toString(), isNot(contains('connectionToken')));
    expect(HttpStatusException(500, '/x').isAuthError, isFalse);
  });
}
