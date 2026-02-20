import 'package:collection/collection.dart';
import 'package:recon/apis/session_api.dart';
import 'package:recon/clients/api_client.dart';
import 'package:recon/clients/settings_client.dart';
import 'package:recon/models/session.dart';
import 'package:flutter/foundation.dart';

class SessionClient extends ChangeNotifier {
  final ApiClient apiClient;
  final SettingsClient settingsClient;

  Future<List<Session>>? _sessionsFuture;
  Session? _currentSession;

  SessionFilterSettings _filterSettings = SessionFilterSettings.empty();

  SessionClient({required this.apiClient, required this.settingsClient}) {
    _filterSettings = SessionFilterSettings(
      name: "",
      hostName: "",
      includeEnded: settingsClient.currentSettings.sessionViewLastIncludeEnded.valueOrDefault,
      includeIncompatible: settingsClient.currentSettings.sessionViewLastIncludeIncompatible.valueOrDefault,
      minActiveUsers: settingsClient.currentSettings.sessionViewLastMinimumUsers.valueOrDefault,
      includeEmptyHeadless: settingsClient.currentSettings.sessionViewLastIncludeEmpty.valueOrDefault,
    );
  }

  SessionFilterSettings get filterSettings => _filterSettings;

  Future<List<Session>>? get sessionsFuture => _sessionsFuture;

  /// Session the current user is in (hosting or present), if any. Updated by [refreshCurrentSession].
  Session? get currentSession => _currentSession;

  set filterSettings(value) {
    _filterSettings = value;
    reloadSessions();
  }

  void initSessions() {
    _sessionsFuture = SessionApi.getSessions(apiClient, filterSettings: _filterSettings).then(
          (value) => value.sorted(
            (a, b) => b.sessionUsers.length.compareTo(a.sessionUsers.length),
          ),
        );
    refreshCurrentSession();
  }

  void reloadSessions() {
    initSessions();
    notifyListeners();
  }

  /// Fetches sessions where the current user is host or present, and sets [currentSession]
  /// to the first live one. Call when you need to show "in game" state (e.g. "Now" tab).
  Future<void> refreshCurrentSession() async {
    final userId = apiClient.userId;
    if (userId.isEmpty) {
      _currentSession = null;
      notifyListeners();
      return;
    }
    final filter = SessionFilterSettings(
      name: "",
      includeEnded: false,
      includeIncompatible: false,
      hostName: userId,
      minActiveUsers: 0,
      includeEmptyHeadless: true,
    );
    try {
      final list = await SessionApi.getSessions(apiClient, filterSettings: filter);
      final live = list.where((s) => s.isLive).toList();
      final was = _currentSession?.id;
      _currentSession = live.isNotEmpty ? live.first : null;
      if (_currentSession?.id != was) notifyListeners();
    } catch (_) {
      _currentSession = null;
      notifyListeners();
    }
  }
}
