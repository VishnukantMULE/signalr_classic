/// Drop-in replacement for `package:signalr_flutter/signalr_flutter.dart`.
///
/// Change the import from `package:signalr_flutter/signalr_flutter.dart` to
/// `package:signalr_classic/signalr_flutter_compat.dart`; the `SignalR`
/// constructor, `connect`, `reconnect`, `stop`, `isConnected`,
/// `invokeMethod`, `ConnectionStatus` and `Transport` keep the same shape.
library signalr_flutter_compat;

export 'src/compat/signalr_flutter_compat.dart';
export 'src/types.dart' show LogLevel, SignalRLogger;
