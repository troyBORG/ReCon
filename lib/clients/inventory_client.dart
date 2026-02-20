import 'dart:async';

import 'package:collection/collection.dart';
import 'package:flutter/material.dart';
import 'package:recon/apis/record_api.dart';
import 'package:recon/clients/api_client.dart';
import 'package:recon/models/inventory/resonite_directory.dart';
import 'package:recon/models/records/record.dart';
import 'package:recon/utils/fuzzy_match.dart';

enum SortMode {
  name,
  date,
  resonite;

  int sortFunction(Record a, Record b, {bool reverse = false}) {
    final func = switch (this) {
      SortMode.name => (Record x, Record y) => x.formattedName
          .toString()
          .toLowerCase()
          .compareTo(y.formattedName.toString().toLowerCase()),
      SortMode.date => (Record x, Record y) =>
          x.creationTime.compareTo(y.creationTime),
      SortMode.resonite => (Record x, Record y) {
          if (x.isItem && !y.isItem) return 1;
          if (!x.isItem && y.isItem) return -1;
          return x.isItem
              ? x.creationTime.compareTo(y.creationTime)
              : x.name.toLowerCase().compareTo(y.name.toLowerCase());
        },
    };
    if (reverse) {
      return func(b, a);
    }
    return func(a, b);
  }

  static const Map<SortMode, IconData> _iconsMap = {
    SortMode.name: Icons.sort_by_alpha,
    SortMode.date: Icons.access_time_outlined,
    SortMode.resonite: Icons.star_border_purple500_sharp,
  };

  IconData get icon => _iconsMap[this] ?? Icons.question_mark;
}

class InventoryClient extends ChangeNotifier {
  final ApiClient apiClient;
  final Map<String, Record> _selectedRecords = {};

  Future<ResoniteDirectory>? _currentDirectory;
  SortMode _sortMode = SortMode.resonite;
  bool _sortReverse = false;
  String _searchQuery = "";

  InventoryClient({required this.apiClient});

  String get searchQuery => _searchQuery;

  set searchQuery(String value) {
    final trimmed = value.trim();
    if (_searchQuery == trimmed) return;
    _searchQuery = trimmed;
    notifyListeners();
  }

  void clearSearch() {
    if (_searchQuery.isEmpty) return;
    _searchQuery = "";
    notifyListeners();
  }

  SortMode get sortMode => _sortMode;

  bool get sortReverse => _sortReverse;

  set sortMode(SortMode mode) {
    if (_sortMode != mode) {
      _sortMode = mode;
      _ensureDirectorySorted();
      notifyListeners();
    }
  }

  set sortReverse(bool reverse) {
    if (_sortReverse != reverse) {
      _sortReverse = reverse;
      notifyListeners();
    }
  }

  List<Record> get selectedRecords => _selectedRecords.values.toList();

  Future<ResoniteDirectory>? get directoryFuture => _currentDirectory;

  void _ensureDirectorySorted() {
    _currentDirectory = _currentDirectory?.then(
      (value) {
        value.children.sort(
          (a, b) =>
              _sortMode.sortFunction(a.record, b.record, reverse: _sortReverse),
        );
        return value;
      },
    );
  }

  bool get isAnyRecordSelected => _selectedRecords.isNotEmpty;

  bool isRecordSelected(Record record) =>
      _selectedRecords.containsKey(record.id);

  int get selectedRecordCount => _selectedRecords.length;

  bool get onlyFilesSelected => _selectedRecords.values.every((element) =>
      element.recordType != RecordType.link &&
      element.recordType != RecordType.directory);

  void clearSelectedRecords() {
    _selectedRecords.clear();
    notifyListeners();
  }

  Future<void> deleteSelectedRecords() async {
    for (final recordId in _selectedRecords.keys) {
      await RecordApi.deleteRecord(apiClient, recordId: recordId);
    }
    _selectedRecords.clear();
    unawaited(reloadCurrentDirectory());
  }

  void toggleRecordSelected(Record record) {
    if (_selectedRecords.containsKey(record.id)) {
      _selectedRecords.remove(record.id);
    } else {
      _selectedRecords[record.id] = record;
    }
    notifyListeners();
  }

  /// Path separator used by the API for [record] (backend may use \ or /).
  static String _pathSep(String path) => path.contains('/') ? '/' : r'\';

  Future<List<Record>> _getDirectory(Record record) async {
    ResoniteDirectory? dir;
    try {
      dir = await _currentDirectory;
    } catch (_) {}
    final List<Record> records;
    if (dir == null || record.isRoot) {
      records = await RecordApi.getUserRecordsAt(
        apiClient,
        path: ResoniteDirectory.rootName,
      );
    } else {
      final sep = _pathSep(record.path);
      if (record.recordType == RecordType.link) {
        if (record.isGroupRecord) {
          final linkRecord = await RecordApi.getGroupRecordByPath(
            apiClient,
            path: "root/${record.path}/${record.name}",
            groupId: record.linkOwnerId,
          );
          final linkSep = _pathSep(linkRecord.path);
          records = await RecordApi.getGroupRecordsAt(
            apiClient,
            path: "${linkRecord.path}$linkSep${linkRecord.name}",
            groupId: linkRecord.ownerId,
          );
        } else {
          final linkRecord = await RecordApi.getUserRecord(
            apiClient,
            recordId: record.linkRecordId,
            user: record.linkOwnerId,
          );
          final linkSep = _pathSep(linkRecord.path);
          records = await RecordApi.getUserRecordsAt(
            apiClient,
            path: "${linkRecord.path}$linkSep${linkRecord.name}",
            user: linkRecord.ownerId,
          );
        }
      } else {
        records = await RecordApi.getUserRecordsAt(apiClient,
            path: "${record.path}$sep${record.name}", user: record.ownerId);
      }
    }
    return records;
  }

  void loadInventoryRoot() {
    final rootRecord = Record.inventoryRoot();
    final rootFuture = _getDirectory(rootRecord).then(
      (records) {
        final rootDir = ResoniteDirectory(
          record: rootRecord,
          children: [],
        );
        rootDir.children.addAll(
          records
              .map((e) =>
                  ResoniteDirectory.fromRecord(record: e, parent: rootDir))
              .toList(),
        );
        return rootDir;
      },
    );
    _currentDirectory = rootFuture;
    _ensureDirectorySorted();
  }

  void forceNotify() => notifyListeners();

  Future<List<Record>> getDirectoryRecords(Record record) =>
      _getDirectory(record);

  String _recordFullPath(Record record) {
    if (record.isRoot) {
      return ResoniteDirectory.rootName;
    }
    if (record.path.isEmpty) {
      return record.name;
    }
    final sep = _pathSep(record.path);
    return "${record.path}$sep${record.name}";
  }

  /// Human-readable path for a record (e.g. "Inventory / Folder / item").
  String recordDisplayPath(Record record) {
    final raw = _recordFullPath(record);
    return raw.replaceAll(_pathSplitter, " / ");
  }

  Future<void> copySelectedRecordsTo(Record targetDirectory) async {
    final selected = _selectedRecords.values.toList();
    await _copyRecords(selected, targetDirectory);
    await reloadCurrentDirectory();
  }

  Future<void> moveSelectedRecordsTo(Record targetDirectory) async {
    final selected = _selectedRecords.values.toList();
    await _copyRecords(selected, targetDirectory);
    for (final record in selected) {
      await RecordApi.deleteRecord(apiClient, recordId: record.id);
    }
    _selectedRecords.clear();
    await reloadCurrentDirectory();
  }

  Future<void> _copyRecords(
      List<Record> records, Record targetDirectory) async {
    if (records.isEmpty) {
      return;
    }
    if (!targetDirectory.isRoot &&
        targetDirectory.recordType != RecordType.directory) {
      throw "Target is not a directory.";
    }
    final targetPath = _recordFullPath(targetDirectory);
    final now = DateTime.now().toUtc();
    for (final record in records) {
      if (record.recordType == RecordType.directory ||
          record.recordType == RecordType.link) {
        throw "Copying directories or links is not supported yet.";
      }
      final newId = Record.generateId();
      final duplicate = record.copyWith(
        id: newId,
        combinedRecordId:
            RecordId(id: newId, ownerId: apiClient.userId, isValid: true),
        ownerId: apiClient.userId,
        path: targetPath,
        url: "resrec:///${apiClient.userId}/$newId",
        lastModificationTime: now,
        creationTime: now,
        fetchedOn: now,
        lastModifyingUserId: apiClient.userId,
        isSynced: false,
        globalVersion: 0,
        localVersion: 1,
      );
      await RecordApi.upsertRecord(apiClient, record: duplicate);
    }
  }

  Future<Record> createDirectory(
      {required Record parent, required String name}) async {
    final trimmedName = name.trim();
    if (trimmedName.isEmpty) {
      throw "Folder name cannot be empty.";
    }
    if (trimmedName.length > 128) {
      throw "Folder name is too long.";
    }
    if (RegExp(r'[\\/:*?"<>|]').hasMatch(trimmedName)) {
      throw "Folder name contains invalid characters.";
    }
    if (!parent.isRoot && parent.recordType != RecordType.directory) {
      throw "Cannot create a folder here.";
    }
    final newId = Record.generateId();
    final timestamp = DateTime.now().toUtc();
    final path = _recordFullPath(parent);
    final newDirectory = Record(
      id: newId,
      combinedRecordId:
          RecordId(id: newId, ownerId: apiClient.userId, isValid: true),
      ownerId: apiClient.userId,
      assetUri: "",
      globalVersion: 0,
      localVersion: 1,
      name: trimmedName,
      description: '',
      tags: [trimmedName],
      recordType: RecordType.directory,
      thumbnailUri: '',
      isPublic: false,
      isForPatreons: false,
      isListed: false,
      lastModificationTime: timestamp,
      resoniteDBManifest: const [],
      lastModifyingUserId: apiClient.userId,
      lastModifyingMachineId: '',
      creationTime: timestamp,
      manifest: const [],
      url: "resrec:///${apiClient.userId}/$newId",
      isValidOwnerId: true,
      isValidRecordId: true,
      visits: 0,
      rating: 0,
      randomOrder: 0,
      fetchedOn: timestamp,
      isSynced: false,
      path: path,
    );
    await RecordApi.upsertRecord(apiClient, record: newDirectory);
    return newDirectory;
  }

  Future<void> reloadCurrentDirectory() async {
    final dir = await _currentDirectory;

    if (dir == null) {
      throw "Failed to reload: No directory loaded.";
    }

    _currentDirectory = _getDirectory(dir.record).then(
      (records) {
        final children = records
            .map((record) =>
                ResoniteDirectory.fromRecord(record: record, parent: dir))
            .toList();
        final newDir = ResoniteDirectory(
            record: dir.record, children: children, parent: dir.parent);

        final parentIdx = dir.parent?.children.indexOf(dir) ?? -1;
        if (parentIdx != -1) {
          dir.parent?.children[parentIdx] = newDir;
        }
        return newDir;
      },
    ).onError((error, stackTrace) {
      return dir;
    });
    _ensureDirectorySorted();
    notifyListeners();
  }

  Future<void> navigateTo(Record record) async {
    final dir = await _currentDirectory;

    if (dir == null) {
      throw "Failed to open: No directory loaded.";
    }

    if (record.recordType != RecordType.directory &&
        record.recordType != RecordType.link) {
      throw "Failed to open: Record is not a directory.";
    }

    final childDir = dir.findChildByRecord(record);
    if (childDir == null) {
      throw "Failed to open: Record is not a child of current directory.";
    }

    Object? caughtError;

    if (childDir.isLoaded) {
      _currentDirectory = Future.value(childDir);
    } else {
      _currentDirectory = _getDirectory(record).then(
        (records) {
          childDir.children.clear();
          childDir.children.addAll(records.map((record) =>
              ResoniteDirectory.fromRecord(record: record, parent: childDir)));
          return childDir;
        },
      ).onError((error, stackTrace) {
        caughtError = error;
        return dir;
      });
    }
    _ensureDirectorySorted();
    notifyListeners();
    await _currentDirectory;
    // Dirty hack to throw the error here instead of letting the FutureBuilder handle it. This means we can keep showing
    // the previous directory while also being able to display the error as a snackbar.
    if (caughtError != null) {
      throw caughtError!;
    }
  }

  /// Navigate to the folder that contains [record] (opens that folder so the record is visible).
  Future<void> navigateToContainingFolder(Record record) async {
    final path = record.path.trim();
    if (path.isEmpty) {
      loadInventoryRoot();
      notifyListeners();
      return;
    }
    return navigateToPath(path);
  }

  /// Navigate so that [record] (a directory or link) is the current folder.
  Future<void> navigateToRecordFolder(Record record) async {
    final sep = record.path.isEmpty ? '' : _pathSep(record.path);
    final path = record.path.isEmpty ? record.name : "${record.path}$sep${record.name}";
    return navigateToPath(path.trim());
  }

  static final _pathSplitter = RegExp(r'[\\/]');

  /// Navigate to the folder at [path] (e.g. "Inventory" or "Inventory\\Subfolder").
  /// Used to open the folder containing a search result.
  Future<void> navigateToPath(String path) async {
    final trimmed = path.trim();
    if (trimmed.isEmpty || trimmed == ResoniteDirectory.rootName) {
      loadInventoryRoot();
      notifyListeners();
      return;
    }
    final segments = trimmed.split(_pathSplitter);
    if (segments.isEmpty || segments.first != ResoniteDirectory.rootName) {
      loadInventoryRoot();
      notifyListeners();
      return;
    }
    Record current = Record.inventoryRoot();
    final pathDirs = <ResoniteDirectory>[];
    pathDirs.add(ResoniteDirectory(record: current, parent: null, children: []));
    for (var i = 1; i < segments.length; i++) {
      final children = await _getDirectory(current);
      final name = segments[i];
      final childRecord = children.where((r) => r.name == name).firstOrNull;
      if (childRecord == null) {
        loadInventoryRoot();
        notifyListeners();
        return;
      }
      current = childRecord;
      final parentDir = pathDirs.last;
      final childDir = ResoniteDirectory(record: current, parent: parentDir, children: []);
      parentDir.children.add(childDir);
      pathDirs.add(childDir);
    }
    final records = await _getDirectory(current);
    pathDirs.last.children
        .addAll(records.map((r) => ResoniteDirectory.fromRecord(record: r, parent: pathDirs.last)));
    _currentDirectory = Future.value(pathDirs.last);
    _ensureDirectorySorted();
    notifyListeners();
  }

  Future<void> navigateUp({int times = 1}) async {
    if (times == 0) return;

    var dir = await _currentDirectory;
    if (dir == null) {
      throw "Failed to navigate up: No directory loaded.";
    }
    if (dir.record.isRoot) {
      throw "Failed navigate up: Already at root";
    }

    for (var i = 0; i < times; i++) {
      dir = dir?.parent;
    }

    _currentDirectory = Future.value(dir);
    _ensureDirectorySorted();
    notifyListeners();
  }
}
