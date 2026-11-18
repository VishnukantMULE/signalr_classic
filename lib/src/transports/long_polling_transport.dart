import 'dart:async';
import 'dart:io';

import '../exceptions.dart';
import '../types.dart';
import 'transport.dart';

/// Long polling: `/connect` returns the init message, then `/poll` requests
/// are held open by the server until there is data (or ConnectionTimeout).
/// No keep-alives on this transport.
class LongPollingTransport implements ClientTransport {
  LongPollingTransport(this._host);

  final TransportHost _host;
  HttpClientRequest? _current;
  bool _closed = false;

  @override
  TransportType get type => TransportType.longPolling;

  @override
  bool get supportsKeepAlive => false;

  @override
  Future<void> start({required bool reconnect}) async {
    if (!reconnect) {
      final body = await _host.http.get(
        _host.connectUri(type, reconnect: false),
        timeout: _host.transportConnectTimeout,
        onRequest: (r) => _current = r,
      );
      if (_closed) throw TransportException('Transport closed while opening');
      final info = _host.onTransportData(this, body);
      unawaited(_pollLoop(reconnectFirst: info.shouldReconnect, initialDelay: info.longPollDelay));
      return;
    }
    // Like the JavaScript client: the reconnect poll counts as reconnected when
    // it succeeds, or when it is still open after the reconnect delay.
    final started = Completer<void>();
    unawaited(_pollLoop(reconnectFirst: true, started: started));
    await started.future;
  }

  Future<void> _pollLoop({
    required bool reconnectFirst,
    Duration? initialDelay,
    Completer<void>? started,
  }) async {
    var reconnect = reconnectFirst;
    var delay = initialDelay ?? _host.longPollDelay;
    while (!_closed) {
      if (delay > Duration.zero) {
        await Future<void>.delayed(delay);
        if (_closed) return;
      }
      Timer? grace;
      if (started != null && !started.isCompleted) {
        grace = Timer(_host.reconnectDelay, () {
          if (!_closed && !started.isCompleted) started.complete();
        });
      }
      try {
        _host.markActive();
        final body = await _host.http.get(
          _host.pollUri(reconnect: reconnect),
          timeout: _host.pollTimeout,
          onRequest: (r) => _current = r,
        );
        grace?.cancel();
        if (_closed) return;
        if (started != null && !started.isCompleted) started.complete();
        final info = _host.onTransportData(this, body);
        if (info.ignored || info.disconnect) return;
        reconnect = info.shouldReconnect;
        delay = info.longPollDelay ?? _host.longPollDelay;
      } catch (e) {
        grace?.cancel();
        if (_closed) return;
        _closed = true;
        if (started != null && !started.isCompleted) {
          started.completeError(e);
          return;
        }
        _host.onTransportLost(this, e is TransportException ? e : TransportException('Long poll failed', e));
        return;
      }
    }
  }

  @override
  Future<void> send(String payload) {
    if (_closed) return Future<void>.error(TransportException('Long polling transport is closed'));
    return sendOverHttp(_host, this, payload);
  }

  @override
  Future<void> close() async {
    _closed = true;
    _current?.abort();
    _current = null;
  }
}
