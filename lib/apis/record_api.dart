import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';
import 'dart:ui' as ui;
import 'package:collection/collection.dart';
import 'package:recon/models/records/asset_digest.dart';
import 'package:recon/models/records/json_template.dart';
import 'package:http/http.dart' as http;
import 'package:flutter/material.dart';
import 'package:logging/logging.dart';

import 'package:recon/clients/api_client.dart';
import 'package:recon/models/records/asset_upload_data.dart';
import 'package:recon/models/records/resonite_db_asset.dart';
import 'package:recon/models/records/preprocess_status.dart';
import 'package:recon/models/records/record.dart';
import 'package:http_parser/http_parser.dart';
import 'package:path/path.dart';
import 'package:recon/models/records/search_sort.dart';

class RecordApi {
  static final Logger _logger = Logger("RecordApi");
  static Future<Record> getUserRecord(ApiClient client, {required String recordId, String? user}) async {
    final response = await client.get("/users/${user ?? client.userId}/records/$recordId");
    client.checkResponse(response);
    final body = jsonDecode(response.body) as Map;
    return Record.fromMap(body);
  }

  static Future<Record> getGroupRecordByPath(ApiClient client, {required String path, required String groupId}) async {
    final response = await client.get("/groups/$groupId/records/$path");
    client.checkResponse(response);
    final body = jsonDecode(response.body) as Map;
    return Record.fromMap(body);
  }

  static Future<List<Record>> searchWorldRecords(
    ApiClient client, {
    List<String> requiredTags = const [],
    SearchSortDirection sortDirection = SearchSortDirection.descending,
    SearchSortParameter sortParameter = SearchSortParameter.lastUpdateDate,
    int limit = 10,
    int offset = 0,
  }) async {
    final requestBody = {
      "requiredTags": requiredTags,
      "sortDirection": sortDirection.toString(),
      "sortBy": sortParameter.serialize(),
      "count": limit,
      "offset": offset,
      "recordType": "world",
    };
    final response = await client.post("/records/pagedSearch", body: jsonEncode(requestBody));
    client.checkResponse(response);
    final body = (jsonDecode(response.body) as Map)["records"] as List;
    return body.map((e) => Record.fromMap(e)).toList();
  }

  static Future<List<Record>> getUserRecordsAt(ApiClient client, {required String path, String? user}) async {
    final encodedPath = Uri.encodeComponent(path);
    final response = await client.get("/users/${user ?? client.userId}/records?path=$encodedPath");
    client.checkResponse(response);
    final body = jsonDecode(response.body) as List;
    return body.map((e) => Record.fromMap(e)).toList();
  }

  static Future<List<Record>> getGroupRecordsAt(ApiClient client, {required String path, required String groupId}) async {
    final response = await client.get("/groups/$groupId/records?path=$path");
    client.checkResponse(response);
    final body = jsonDecode(response.body) as List;
    return body.map((e) => Record.fromMap(e)).toList();
  }

  static Future<void> deleteRecord(ApiClient client, {required String recordId}) async {
    final response = await client.delete("/users/${client.userId}/records/$recordId");
    client.checkResponse(response);
  }

  static Future<PreprocessStatus> preprocessRecord(ApiClient client, {required Record record}) async {
    final body = jsonEncode(record.toMap());
    final response = await client.post("/users/${record.ownerId}/records/${record.id}/preprocess", body: body);
    client.checkResponse(response);
    final resultBody = jsonDecode(response.body);
    return PreprocessStatus.fromMap(resultBody);
  }

  static Future<PreprocessStatus> getPreprocessStatus(ApiClient client, {required PreprocessStatus preprocessStatus}) async {
    final response = await client.get("/users/${preprocessStatus.ownerId}/records/${preprocessStatus.recordId}/preprocess/${preprocessStatus.id}");
    client.checkResponse(response);
    final body = jsonDecode(response.body);
    return PreprocessStatus.fromMap(body);
  }

  static Future<PreprocessStatus> tryPreprocessRecord(ApiClient client, {required Record record}) async {
    var status = await preprocessRecord(client, record: record);
    while (status.state == RecordPreprocessState.preprocessing) {
      await Future.delayed(const Duration(seconds: 1));
      status = await getPreprocessStatus(client, preprocessStatus: status);
    }

    if (status.state != RecordPreprocessState.success) {
      throw "Record Preprocessing failed: ${status.failReason}";
    }
    return status;
  }

  static Future<AssetUploadData> beginUploadAsset(ApiClient client, {required ResoniteDBAsset asset}) async {
    // Try to begin the upload with retries in case of transient errors
    int retries = 3;
    Exception? lastException;
    
    while (retries > 0) {
      try {
        final response = await client.post("/users/${client.userId}/assets/${asset.hash}/chunks");
        client.checkResponse(response);
        final body = jsonDecode(response.body);
        final res = AssetUploadData.fromMap(body);
        if (res.uploadState == UploadState.failed) {
          throw "Asset upload failed: ${res.uploadState.name}";
        }
        return res;
      } catch (e) {
        lastException = e is Exception ? e : Exception(e.toString());
        // If it's a 404, the asset endpoint might not exist yet - wait and retry
        if (e.toString().contains("404") || e.toString().contains("Resource not found")) {
          _logger.warning("beginUploadAsset: Got 404 for asset ${asset.hash}, retrying... (${4 - retries}/3)");
          await Future.delayed(const Duration(milliseconds: 500));
          retries--;
        } else {
          // For other errors, don't retry
          rethrow;
        }
      }
    }
    
    // If we exhausted retries, throw the last exception
    throw lastException ?? Exception("Failed to begin asset upload after retries");
  }

  static Future<Record?> upsertRecord(ApiClient client, {required Record record, bool ensureFolder = true}) async {
    // For message records (path is null or empty), don't ensure folder structure
    final shouldEnsureFolder = ensureFolder && record.path != null && record.path!.isNotEmpty;
    final url = "/users/${client.userId}/records/${record.id}?ensureFolder=$shouldEnsureFolder";
    
    final body = jsonEncode(record.toMap());
    final response = await client.put(url, body: body);
    client.checkResponse(response);
    
    // The PUT response returns CloudMessage, not the record itself
    // So we can't get the updated record from the response
    // We'll need to fetch it separately
    return null;
  }

  static Future<void> uploadAsset(ApiClient client,
      {required AssetUploadData uploadData,
      required String filename,
      required ResoniteDBAsset asset,
      required Uint8List data,
      void Function(double number)? progressCallback}) async {
    for (int i = 0; i < uploadData.totalChunks; i++) {
      progressCallback?.call(i / uploadData.totalChunks);
      final offset = i * uploadData.chunkSize;
      final end = (i + 1) * uploadData.chunkSize;
      final request = http.MultipartRequest(
        "POST",
        ApiClient.buildFullUri("/users/${client.userId}/assets/${asset.hash}/chunks/$i"),
      )
        ..files.add(
            http.MultipartFile.fromBytes("file", data.getRange(offset, min(end, data.length)).toList(), filename: filename, contentType: MediaType.parse("multipart/form-data")))
        ..headers.addAll(client.authorizationHeader);
      final response = await request.send();
      final bodyBytes = await response.stream.toBytes();
      client.checkResponse(http.Response.bytes(bodyBytes, response.statusCode));
      progressCallback?.call(1);
    }
  }

  static Future<void> finishUpload(ApiClient client, {required ResoniteDBAsset asset}) async {
    final response = await client.patch("/users/${client.userId}/assets/${asset.hash}/chunks");
    client.checkResponse(response);
  }

  static Future<void> uploadAssets(ApiClient client, {required List<AssetDigest> assets, void Function(double progress)? progressCallback}) async {
    progressCallback?.call(0);
    for (int i = 0; i < assets.length; i++) {
      final totalProgress = i / assets.length;
      progressCallback?.call(totalProgress);
      final entry = assets[i];
      final uploadData = await beginUploadAsset(client, asset: entry.asset);
      if (uploadData.uploadState == UploadState.failed) {
        throw "Asset upload failed: ${uploadData.uploadState.name}";
      }
      await uploadAsset(
        client,
        uploadData: uploadData,
        asset: entry.asset,
        data: entry.data,
        filename: entry.name,
        progressCallback: (progress) => progressCallback?.call(totalProgress + progress * 1 / assets.length),
      );
      await finishUpload(client, asset: entry.asset);
    }
    progressCallback?.call(1);
  }

  static Future<Record> uploadImage(ApiClient client, {required File image, required String machineId, String? messageId, void Function(double progress)? progressCallback}) async {
    _logger.info("uploadImage: Starting upload for ${basename(image.path)}, messageId: $messageId");
    try {
      progressCallback?.call(0);
      final imageDigest = await AssetDigest.fromData(await image.readAsBytes(), basename(image.path));
      _logger.info("uploadImage: Image digest created: ${imageDigest.asset.hash}");
    
    // Try to decode image to get dimensions, fallback to default if it fails (e.g., WebP issues)
    int width = 1024;
    int height = 1024;
    try {
      final codec = await ui.instantiateImageCodec(imageDigest.data);
      final frame = await codec.getNextFrame();
      width = frame.image.width;
      height = frame.image.height;
      frame.image.dispose();
    } catch (e) {
      // If decoding fails (e.g., unsupported format), use default dimensions
      // The image will still upload and work, just with default aspect ratio
    }
    
    final filename = basenameWithoutExtension(image.path);

    final objectJson = jsonEncode(JsonTemplate.image(imageUri: imageDigest.dbUri, filename: filename, width: width, height: height).data);
    final objectBytes = Uint8List.fromList(utf8.encode(objectJson));

    final objectDigest = await AssetDigest.fromData(objectBytes, "${basenameWithoutExtension(image.path)}.brson");

    final digests = [imageDigest, objectDigest];

    final record = Record.fromRequiredData(
      recordType: RecordType.object,  // Use 'object' type so it spawns as an object in-game
      userId: client.userId,
      machineId: machineId,
      assetUri: objectDigest.dbUri,
      filename: filename,
      thumbnailUri: imageDigest.dbUri,
      digests: digests,
      extraTags: ["image"],
      messageId: messageId,  // Include message ID in tags for message records
      path: null,  // null path for message records (will serialize as null)
    );
    _logger.info("uploadImage: Record created with ID: ${record.id}, path: ${record.path}, tags: ${record.tags}");
    progressCallback?.call(.1);
    _logger.info("uploadImage: Starting preprocess");
    final status = await tryPreprocessRecord(client, record: record);
    final toUpload = status.resultDiffs.whereNot((element) => element.isUploaded);
    _logger.info("uploadImage: Preprocess complete, ${toUpload.length} assets to upload");
    _logger.info("uploadImage: Image digest hash: ${imageDigest.asset.hash}, Object digest hash: ${objectDigest.asset.hash}");
    _logger.info("uploadImage: Preprocess diffs: ${status.resultDiffs.map((d) => '${d.hash} (uploaded: ${d.isUploaded})').join(', ')}");
    
    // Ensure we upload both the image and the object file
    final assetsToUpload = digests.where((digest) => toUpload.any((diff) => digest.asset.hash == diff.hash)).toList();
    _logger.info("uploadImage: Assets to upload: ${assetsToUpload.map((a) => '${a.asset.hash} (${a.name})').join(', ')}");
    
    // Check if preprocessing says assets are already uploaded
    final alreadyUploaded = digests.where((digest) => !toUpload.any((diff) => digest.asset.hash == diff.hash)).toList();
    if (alreadyUploaded.isNotEmpty) {
      _logger.info("uploadImage: Some assets are already uploaded (skipping): ${alreadyUploaded.map((a) => '${a.asset.hash} (${a.name})').join(', ')}");
    }
    
    // If no assets need uploading but we have digests, something is wrong - log a warning
    if (assetsToUpload.isEmpty && digests.isNotEmpty) {
      _logger.warning("uploadImage: No assets to upload but we have ${digests.length} digests. This might indicate the assets are already on the server.");
    }
    
    progressCallback?.call(.2);

    _logger.info("uploadImage: Starting asset upload");
    await uploadAssets(client,
        assets: assetsToUpload,
        progressCallback: (progress) => progressCallback?.call(.2 + progress * .6));
    _logger.info("uploadImage: Assets uploaded, upserting record");
    final upsertedRecord = await upsertRecord(client, record: record);
    Record syncedRecord = upsertedRecord ?? record; // Use response if available, otherwise original
    
    if (upsertedRecord != null && upsertedRecord.globalVersion >= 1) {
      _logger.info("uploadImage: Record upserted with globalVersion: ${upsertedRecord.globalVersion}, using it directly");
    } else {
      _logger.info("uploadImage: Record upserted, waiting for sync (response globalVersion: ${upsertedRecord?.globalVersion ?? 'null'})");
      
      // Wait a moment for the server to process the record, then fetch it back to ensure it's synced
      await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer
      int retries = 5; // More retries
      while (retries > 0 && syncedRecord.globalVersion < 1) {
        try {
          _logger.info("uploadImage: Fetching synced record (attempt ${6 - retries}/5)");
          final fetchedRecord = await getUserRecord(client, recordId: record.id);
          _logger.info("uploadImage: Successfully fetched synced record, globalVersion: ${fetchedRecord.globalVersion}, localVersion: ${fetchedRecord.localVersion}");
          
          // Preserve our original lastModifyingUserId and lastModifyingMachineId if the API returned null/empty
          syncedRecord = fetchedRecord.copyWith(
            lastModifyingUserId: fetchedRecord.lastModifyingUserId.isEmpty ? record.lastModifyingUserId : fetchedRecord.lastModifyingUserId,
            lastModifyingMachineId: fetchedRecord.lastModifyingMachineId.isEmpty ? record.lastModifyingMachineId : fetchedRecord.lastModifyingMachineId,
          );
          
          // If we got globalVersion: 1, we're good
          if (syncedRecord.globalVersion >= 1) {
            break;
          } else {
            _logger.info("uploadImage: Record not fully synced yet (globalVersion: ${syncedRecord.globalVersion}), waiting...");
            await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer between retries
            retries--;
          }
        } catch (e, s) {
          _logger.warning("uploadImage: Failed to fetch synced record: $e", e, s);
          retries--;
          if (retries > 0) {
            await Future.delayed(const Duration(milliseconds: 1000));
          } else {
            // If all retries fail, use what we have
            syncedRecord = upsertedRecord ?? record;
          }
        }
      }
    }
    progressCallback?.call(1);
    _logger.info("uploadImage: Upload complete, returning record with globalVersion: ${syncedRecord.globalVersion}");
    return syncedRecord;
    } catch (e, s) {
      _logger.severe("uploadImage: Error during upload: $e", e, s);
      rethrow;
    }
  }

  static Future<Record> uploadVoiceClip(ApiClient client, {required File voiceClip, required String machineId, String? messageId, void Function(double progress)? progressCallback}) async {
    _logger.info("uploadVoiceClip: Starting upload for ${basename(voiceClip.path)}, messageId: $messageId");
    try {
      progressCallback?.call(0);
      final voiceDigest = await AssetDigest.fromData(await voiceClip.readAsBytes(), basename(voiceClip.path));
      _logger.info("uploadVoiceClip: Voice digest created: ${voiceDigest.asset.hash}");

    final filename = basenameWithoutExtension(voiceClip.path);
    final digests = [voiceDigest];

    final record = Record.fromRequiredData(
      recordType: RecordType.audio,
      userId: client.userId,
      machineId: machineId,
      assetUri: voiceDigest.dbUri,
      filename: filename,
      thumbnailUri: "",  // Empty string will serialize as null for audio messages
      digests: digests,
      extraTags: ["voice", "message"],
      messageId: messageId,  // Include message ID in tags
      path: null,  // null path for message records (as per docs)
    );
    _logger.info("uploadVoiceClip: Record created with ID: ${record.id}, path: ${record.path}, tags: ${record.tags}, thumbnailUri: ${record.thumbnailUri}");
    progressCallback?.call(.1);
    _logger.info("uploadVoiceClip: Starting preprocess");
    final status = await tryPreprocessRecord(client, record: record);
    final toUpload = status.resultDiffs.whereNot((element) => element.isUploaded);
    _logger.info("uploadVoiceClip: Preprocess complete, ${toUpload.length} assets to upload");
    progressCallback?.call(.2);

    _logger.info("uploadVoiceClip: Starting asset upload");
    await uploadAssets(client,
        assets: digests.where((digest) => toUpload.any((diff) => digest.asset.hash == diff.hash)).toList(),
        progressCallback: (progress) => progressCallback?.call(.2 + progress * .6));
    _logger.info("uploadVoiceClip: Assets uploaded, upserting record");
    final upsertedRecord = await upsertRecord(client, record: record);
    Record syncedRecord = upsertedRecord ?? record; // Use response if available, otherwise original
    
    if (upsertedRecord != null && upsertedRecord.globalVersion >= 1) {
      _logger.info("uploadVoiceClip: Record upserted with globalVersion: ${upsertedRecord.globalVersion}, using it directly");
    } else {
      _logger.info("uploadVoiceClip: Record upserted, waiting for sync (response globalVersion: ${upsertedRecord?.globalVersion ?? 'null'})");
      
      // Wait a moment for the server to process the record, then fetch it back to ensure it's synced
      await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer
      int retries = 5; // More retries
      while (retries > 0 && syncedRecord.globalVersion < 1) {
        try {
          _logger.info("uploadVoiceClip: Fetching synced record (attempt ${6 - retries}/5)");
          final fetchedRecord = await getUserRecord(client, recordId: record.id);
          _logger.info("uploadVoiceClip: Successfully fetched synced record, globalVersion: ${fetchedRecord.globalVersion}, localVersion: ${fetchedRecord.localVersion}");
          
          // Preserve our original lastModifyingUserId and lastModifyingMachineId if the API returned null/empty
          syncedRecord = fetchedRecord.copyWith(
            lastModifyingUserId: fetchedRecord.lastModifyingUserId.isEmpty ? record.lastModifyingUserId : fetchedRecord.lastModifyingUserId,
            lastModifyingMachineId: fetchedRecord.lastModifyingMachineId.isEmpty ? record.lastModifyingMachineId : fetchedRecord.lastModifyingMachineId,
          );
          
          // If we got globalVersion: 1, we're good
          if (syncedRecord.globalVersion >= 1) {
            break;
          } else {
            _logger.info("uploadVoiceClip: Record not fully synced yet (globalVersion: ${syncedRecord.globalVersion}), waiting...");
            await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer between retries
            retries--;
          }
        } catch (e, s) {
          _logger.warning("uploadVoiceClip: Failed to fetch synced record: $e", e, s);
          retries--;
          if (retries > 0) {
            await Future.delayed(const Duration(milliseconds: 1000));
          } else {
            // If all retries fail, use what we have
            syncedRecord = upsertedRecord ?? record;
          }
        }
      }
    }
    progressCallback?.call(1);
    _logger.info("uploadVoiceClip: Upload complete, returning record with globalVersion: ${syncedRecord.globalVersion}");
    return syncedRecord;
    } catch (e, s) {
      _logger.severe("uploadVoiceClip: Error during upload: $e", e, s);
      rethrow;
    }
  }

  static Future<Record> uploadRawFile(ApiClient client, {required File file, required String machineId, String? messageId, void Function(double progress)? progressCallback}) async {
    progressCallback?.call(0);
    final fileDigest = await AssetDigest.fromData(await file.readAsBytes(), basename(file.path));

    final objectJson = jsonEncode(JsonTemplate.rawFile(assetUri: fileDigest.dbUri, filename: fileDigest.name).data);
    final objectBytes = Uint8List.fromList(utf8.encode(objectJson));

    final objectDigest = await AssetDigest.fromData(objectBytes, "${basenameWithoutExtension(file.path)}.brson");

    final digests = [fileDigest, objectDigest];

    final record = Record.fromRequiredData(
      recordType: RecordType.object,  // Use 'object' type so it spawns as an object in-game
      userId: client.userId,
      machineId: machineId,
      assetUri: objectDigest.dbUri,
      filename: fileDigest.name,
      thumbnailUri: JsonTemplate.thumbUrl,
      digests: digests,
      extraTags: ["document"],
      messageId: messageId,  // Include message ID in tags for message records
      path: null,  // null path for message records
    );
    progressCallback?.call(.1);
    final status = await tryPreprocessRecord(client, record: record);
    final toUpload = status.resultDiffs.whereNot((element) => element.isUploaded);
    progressCallback?.call(.2);

    await uploadAssets(client,
        assets: digests.where((digest) => toUpload.any((diff) => digest.asset.hash == diff.hash)).toList(),
        progressCallback: (progress) => progressCallback?.call(.2 + progress * .6));
    final upsertedRecord = await upsertRecord(client, record: record);
    Record syncedRecord = upsertedRecord ?? record; // Use response if available, otherwise original
    
    if (upsertedRecord != null && upsertedRecord.globalVersion >= 1) {
      _logger.info("uploadRawFile: Record upserted with globalVersion: ${upsertedRecord.globalVersion}, using it directly");
    } else {
      _logger.info("uploadRawFile: Record upserted, waiting for sync (response globalVersion: ${upsertedRecord?.globalVersion ?? 'null'})");
      
      // Wait a moment for the server to process the record, then fetch it back to ensure it's synced
      await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer
      int retries = 5; // More retries
      while (retries > 0 && syncedRecord.globalVersion < 1) {
        try {
          _logger.info("uploadRawFile: Fetching synced record (attempt ${6 - retries}/5)");
          final fetchedRecord = await getUserRecord(client, recordId: record.id);
          _logger.info("uploadRawFile: Successfully fetched synced record, globalVersion: ${fetchedRecord.globalVersion}");
          
          // Preserve our original lastModifyingUserId and lastModifyingMachineId if the API returned null/empty
          syncedRecord = fetchedRecord.copyWith(
            lastModifyingUserId: fetchedRecord.lastModifyingUserId.isEmpty ? record.lastModifyingUserId : fetchedRecord.lastModifyingUserId,
            lastModifyingMachineId: fetchedRecord.lastModifyingMachineId.isEmpty ? record.lastModifyingMachineId : fetchedRecord.lastModifyingMachineId,
          );
          
          // If we got globalVersion: 1, we're good
          if (syncedRecord.globalVersion >= 1) {
            break;
          } else {
            _logger.info("uploadRawFile: Record not fully synced yet (globalVersion: ${syncedRecord.globalVersion}), waiting...");
            await Future.delayed(const Duration(milliseconds: 1000)); // Wait longer between retries
            retries--;
          }
        } catch (e, s) {
          _logger.warning("uploadRawFile: Failed to fetch synced record: $e", e, s);
          retries--;
          if (retries > 0) {
            await Future.delayed(const Duration(milliseconds: 1000));
          } else {
            // If all retries fail, use what we have
            syncedRecord = upsertedRecord ?? record;
          }
        }
      }
    }
    progressCallback?.call(1);
    _logger.info("uploadRawFile: Upload complete, returning record with globalVersion: ${syncedRecord.globalVersion}");
    return syncedRecord;
  }
}
