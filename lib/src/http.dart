import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'exceptions.dart';

/// Small HTTP helper around [HttpClient] with timeouts that abort the request.
class SignalRHttp {
  SignalRHttp(this.client, this.headers, this.defaultTimeout);

  final HttpClient client;
  final Map<String, String> headers;
  final Duration defaultTimeout;

  Future<String> get(
    Uri uri, {
    Duration? timeout,
    void Function(HttpClientRequest request)? onRequest,
  }) =>
      _text('GET', uri, null, null, timeout, onRequest);

  Future<String> post(
    Uri uri,
    String body, {
    String contentType = 'application/x-www-form-urlencoded; charset=UTF-8',
    Duration? timeout,
    void Function(HttpClientRequest request)? onRequest,
  }) =>
      _text('POST', uri, body, contentType, timeout, onRequest);

  /// Opens a streaming GET (Server-Sent Events). [timeout] covers receiving
  /// the response headers. The caller owns the returned stream.
  Future<HttpClientResponse> openStream(
    Uri uri, {
    Map<String, String>? extraHeaders,
    Duration? timeout,
    void Function(HttpClientRequest request)? onRequest,
  }) async {
    HttpClientRequest? request;
    try {
      return await () async {
        request = await client.openUrl('GET', uri);
        _applyHeaders(request!, extraHeaders);
        onRequest?.call(request!);
        final response = await request!.close();
        if (response.statusCode != HttpStatus.ok) {
          await response.drain<void>().catchError((Object _) {});
          throw HttpStatusException(response.statusCode, uri.path);
        }
        return response;
      }()
          .timeout(timeout ?? defaultTimeout);
    } on TimeoutException {
      request?.abort();
      throw TransportException('Timed out opening ${uri.path}');
    }
  }

  Future<String> _text(
    String method,
    Uri uri,
    String? body,
    String? contentType,
    Duration? timeout,
    void Function(HttpClientRequest request)? onRequest,
  ) async {
    HttpClientRequest? request;
    try {
      return await () async {
        request = await client.openUrl(method, uri);
        _applyHeaders(request!, null);
        if (contentType != null) request!.headers.set(HttpHeaders.contentTypeHeader, contentType);
        if (body != null) {
          final bytes = utf8.encode(body);
          request!.contentLength = bytes.length;
          request!.add(bytes);
        }
        onRequest?.call(request!);
        final response = await request!.close();
        final text = await response.transform(utf8.decoder).join();
        if (response.statusCode < 200 || response.statusCode > 299) {
          throw HttpStatusException(response.statusCode, uri.path);
        }
        return text;
      }()
          .timeout(timeout ?? defaultTimeout);
    } on TimeoutException {
      request?.abort();
      throw TransportException('Timed out waiting for $method ${uri.path}');
    }
  }

  void _applyHeaders(HttpClientRequest request, Map<String, String>? extra) {
    headers.forEach(request.headers.set);
    extra?.forEach(request.headers.set);
  }
}
