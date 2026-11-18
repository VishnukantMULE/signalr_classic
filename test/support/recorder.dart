import 'dart:async';

import 'package:signalr_classic/signalr_classic.dart';

/// Collects a connection's events for assertions.
class Recorder {
  Recorder(HubConnection connection) {
    _subs
      ..add(connection.onStateChanged.listen((c) => events.add('state:${c.current.name}')))
      ..add(connection.onConnectionSlow.listen((_) => events.add('slow')))
      ..add(connection.onReconnecting.listen((_) => events.add('reconnecting')))
      ..add(connection.onReconnected.listen((_) => events.add('reconnected')))
      ..add(connection.onClosed.listen((e) {
        events.add('closed');
        closedErrors.add(e);
      }))
      ..add(connection.onError.listen((e) {
        events.add('error:${e.kind.name}');
        errors.add(e);
      }));
  }

  final List<String> events = <String>[];
  final List<SignalRError> errors = <SignalRError>[];
  final List<Object?> closedErrors = <Object?>[];
  final List<StreamSubscription<dynamic>> _subs = <StreamSubscription<dynamic>>[];

  Future<void> cancel() async {
    for (final s in _subs) {
      await s.cancel();
    }
  }

  int count(String event) => events.where((e) => e == event).length;

  Future<void> waitFor(String event, {int times = 1, Duration timeout = const Duration(seconds: 5)}) =>
      waitUntil(() => count(event) >= times, timeout: timeout, what: '$event x$times');

  Future<void> waitUntil(bool Function() condition,
      {Duration timeout = const Duration(seconds: 5), String what = 'condition'}) async {
    final end = DateTime.now().add(timeout);
    while (!condition()) {
      if (DateTime.now().isAfter(end)) throw TimeoutException('Timed out waiting for $what; events: $events');
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }
}
