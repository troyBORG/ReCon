import 'dart:convert';
import 'dart:io';

import 'package:background_downloader/background_downloader.dart';
import 'package:cached_network_image/cached_network_image.dart';
import 'package:flutter/material.dart';
import 'package:path/path.dart' as path;
import 'package:path_provider/path_provider.dart';
import 'package:photo_view/photo_view.dart';
import 'package:provider/provider.dart';
import 'package:recon/apis/record_api.dart';
import 'package:recon/auxiliary.dart';
import 'package:recon/client_holder.dart';
import 'package:recon/clients/inventory_client.dart';
import 'package:recon/models/inventory/resonite_directory.dart';
import 'package:recon/models/photo_asset.dart';
import 'package:recon/models/message.dart';
import 'package:recon/models/records/record.dart';
import 'package:recon/string_formatter.dart';
import 'package:recon/widgets/formatted_text.dart';
import 'package:recon/widgets/messages/message_state_indicator.dart';
import 'package:share_plus/share_plus.dart';

class MessageAsset extends StatelessWidget {
  const MessageAsset({required this.message, this.foregroundColor, super.key});

  final Message message;
  final Color? foregroundColor;

  @override
  Widget build(BuildContext context) {
    final content = jsonDecode(message.content);
    final formattedName = FormatNode.fromText(content["name"]);
    return Container(
      constraints: const BoxConstraints(maxWidth: 300),
      child: Column(
        children: [
          SizedBox(
            height: 256,
            width: double.infinity,
            child: CachedNetworkImage(
              imageUrl: Aux.resdbToHttp(content["thumbnailUri"]),
              imageBuilder: (context, image) {
                return InkWell(
                  onTap: () async {
                    PhotoAsset? photoAsset;
                    try {
                      photoAsset = PhotoAsset.fromTags((content["tags"] as List).map((e) => "$e").toList());
                    } catch (_) {}
                    await Navigator.push(
                      context,
                      MaterialPageRoute(
                        builder: (context) => _ImageFullScreenView(
                          message: message,
                          content: content,
                          photoAsset: photoAsset,
                          formattedName: formattedName,
                        ),
                      ),
                    );
                  },
                  child: Hero(
                    tag: message.id,
                    child: ClipRRect(
                      borderRadius: BorderRadius.circular(16),
                      child: Image(
                        image: image,
                        fit: BoxFit.cover,
                      ),
                    ),
                  ),
                );
              },
              errorWidget: (context, url, error) => const Icon(
                Icons.broken_image,
                size: 64,
              ),
              placeholder: (context, uri) => const Center(child: CircularProgressIndicator()),
            ),
          ),
          const SizedBox(
            height: 8,
          ),
          Row(
            crossAxisAlignment: CrossAxisAlignment.end,
            mainAxisSize: MainAxisSize.min,
            children: [
              Expanded(
                child: Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 4.0),
                  child: FormattedText(
                    formattedName,
                    maxLines: null,
                    style: Theme.of(context).textTheme.bodySmall?.copyWith(color: foregroundColor),
                  ),
                ),
              ),
              MessageStateIndicator(
                message: message,
                foregroundColor: foregroundColor,
              ),
            ],
          ),
        ],
      ),
    );
  }
}

class _ImageFullScreenView extends StatefulWidget {
  const _ImageFullScreenView({
    required this.message,
    required this.content,
    this.photoAsset,
    required this.formattedName,
  });

  final Message message;
  final Map<String, dynamic> content;
  final PhotoAsset? photoAsset;
  final FormatNode formattedName;

  @override
  State<_ImageFullScreenView> createState() => _ImageFullScreenViewState();
}

class _ImageFullScreenViewState extends State<_ImageFullScreenView> {
  final Future<Directory> _tempDirectoryFuture = getTemporaryDirectory();
  bool _isSaving = false;
  bool _isSavingToInventory = false;

  String _getImageUri() {
    // Try to get full resolution image from PhotoAsset first
    if (widget.photoAsset != null) {
      return widget.photoAsset!.imageUri;
    }
    // Fall back to thumbnail if no PhotoAsset
    return widget.content["thumbnailUri"] ?? "";
  }

  String _getFilename() {
    final name = widget.content["name"] ?? "image";
    final uri = _getImageUri();
    final ext = path.extension(uri);
    
    // Sanitize filename by replacing invalid characters
    // Windows invalid chars: < > : " | ? * \ /
    // Also replace control characters and normalize
    String sanitizedName = name
        .replaceAll(RegExp(r'[<>:"|?*\\/]'), '-')  // Replace path separators and invalid chars with dash
        .replaceAll(RegExp(r'[\x00-\x1f\x7f]'), '')  // Remove control characters
        .replaceAll(RegExp(r'\s+'), ' ')  // Normalize multiple spaces to single space
        .trim();
    
    // Remove trailing periods and spaces (Windows doesn't allow these)
    sanitizedName = sanitizedName.replaceAll(RegExp(r'[.\s]+$'), '');
    
    // Ensure filename is not empty after sanitization
    if (sanitizedName.isEmpty) {
      sanitizedName = "image";
    }
    
    // Limit filename length (Windows has 255 char limit for full path, but keep it reasonable)
    if (sanitizedName.length > 200) {
      sanitizedName = sanitizedName.substring(0, 200);
    }
    
    return "$sanitizedName${ext.isNotEmpty ? ext : '.jpg'}";
  }

  Future<void> _saveToDevice() async {
    if (_isSaving) return;

    setState(() {
      _isSaving = true;
    });

    try {
      final imageUri = _getImageUri();
      if (imageUri.isEmpty) {
        throw "No image URI found";
      }

      final imageUrl = Aux.resdbToHttp(imageUri);
      final filename = _getFilename();

      // Download to temporary directory first
      final downloadTask = DownloadTask(
        url: imageUrl,
        allowPause: true,
        baseDirectory: BaseDirectory.temporary,
        filename: filename,
        updates: Updates.statusAndProgress,
      );

      final downloadStatus = await FileDownloader().download(downloadTask);
      
      if (downloadStatus.status == TaskStatus.complete) {
        final tempDirectory = await _tempDirectoryFuture;
        final tempFile = File("${tempDirectory.path}/$filename");
        
        if (tempFile.existsSync()) {
          // Try to save to Downloads/Pictures directory
          Directory? targetDirectory;
          try {
            if (Platform.isAndroid) {
              // On Android, use external storage downloads
              final externalDir = await getExternalStorageDirectory();
              if (externalDir != null) {
                targetDirectory = Directory("${externalDir.parent.path}/Download");
                if (!targetDirectory.existsSync()) {
                  targetDirectory = Directory("${externalDir.parent.path}/Downloads");
                }
              }
            } else if (Platform.isWindows || Platform.isLinux || Platform.isMacOS) {
              // On desktop, use Downloads folder
              final homeDir = Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'] ?? '';
              if (homeDir.isNotEmpty) {
                targetDirectory = Directory("$homeDir/Downloads");
              }
            } else {
              // iOS - use documents directory
              targetDirectory = await getApplicationDocumentsDirectory();
            }

            if (targetDirectory != null && !targetDirectory.existsSync()) {
              await targetDirectory.create(recursive: true);
            }

            if (targetDirectory != null && targetDirectory.existsSync()) {
              final targetFile = File("${targetDirectory.path}/$filename");
              
              // Handle filename conflicts
              int counter = 1;
              String finalFilename = filename;
              while (targetFile.existsSync()) {
                final nameWithoutExt = path.basenameWithoutExtension(filename);
                final ext = path.extension(filename);
                finalFilename = "$nameWithoutExt ($counter)$ext";
                counter++;
              }
              
              final finalFile = File("${targetDirectory.path}/$finalFilename");
              await tempFile.copy(finalFile.absolute.path);
              await tempFile.delete();

              if (mounted) {
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(
                    content: Text("Saved to ${finalFile.path}"),
                    action: SnackBarAction(
                      label: "Open",
                      onPressed: () {
                        // Could use url_launcher to open the file location
                      },
                    ),
                  ),
                );
              }
            } else {
              // Fallback: keep in temp and share
              await _shareImage(tempFile);
            }
          } catch (e) {
            // If saving to specific directory fails, try sharing instead
            if (tempFile.existsSync()) {
              await _shareImage(tempFile);
            } else {
              throw e;
            }
          }
        } else {
          throw "Downloaded file not found";
        }
      } else {
        throw downloadStatus.exception ?? "Download failed";
      }
    } catch (e, s) {
      FlutterError.reportError(FlutterErrorDetails(exception: e, stack: s));
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text("Failed to save image: $e")),
        );
      }
    } finally {
      if (mounted) {
        setState(() {
          _isSaving = false;
        });
      }
    }
  }

  Future<void> _shareImage(File file) async {
    try {
      await Share.shareXFiles([XFile(file.path)], text: widget.content["name"] ?? "Image");
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text("Failed to share: $e")),
        );
      }
    }
  }

  Future<void> _shareImageDirect() async {
    if (_isSaving) return;

    setState(() {
      _isSaving = true;
    });

    try {
      final imageUri = _getImageUri();
      if (imageUri.isEmpty) {
        throw "No image URI found";
      }

      final imageUrl = Aux.resdbToHttp(imageUri);
      final filename = _getFilename();

      // Download to temporary directory
      final downloadTask = DownloadTask(
        url: imageUrl,
        allowPause: true,
        baseDirectory: BaseDirectory.temporary,
        filename: filename,
        updates: Updates.statusAndProgress,
      );

      final downloadStatus = await FileDownloader().download(downloadTask);
      
      if (downloadStatus.status == TaskStatus.complete) {
        final tempDirectory = await _tempDirectoryFuture;
        final tempFile = File("${tempDirectory.path}/$filename");
        
        if (tempFile.existsSync()) {
          await _shareImage(tempFile);
        } else {
          throw "Downloaded file not found";
        }
      } else {
        throw downloadStatus.exception ?? "Download failed";
      }
    } catch (e, s) {
      FlutterError.reportError(FlutterErrorDetails(exception: e, stack: s));
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text("Failed to share image: $e")),
        );
      }
    } finally {
      if (mounted) {
        setState(() {
          _isSaving = false;
        });
      }
    }
  }

  Future<void> _saveToInventory() async {
    if (_isSavingToInventory) return;

    setState(() {
      _isSavingToInventory = true;
    });

    try {
      final cHolder = ClientHolder.of(context);
      final inventoryClient = Provider.of<InventoryClient>(context, listen: false);
      
      // Get current inventory directory
      final currentDir = await inventoryClient.directoryFuture;
      if (currentDir == null) {
        throw "No inventory directory loaded";
      }

      // Parse the record from message content
      final record = Record.fromMap(widget.content);
      
      // Build the path for the current directory
      // The API expects paths like "Inventory" for root or "Inventory\\FolderName" for subdirectories
      String targetPath;
      if (currentDir.isRoot) {
        targetPath = ResoniteDirectory.rootName;
      } else {
        // Use the record's path, which should already be in the correct format
        targetPath = currentDir.record.path;
      }

      // Create a new record with the same data but new ID and path
      final newRecord = record.copyWith(
        id: Record.generateId(),
        path: targetPath,
        ownerId: cHolder.apiClient.userId,
        lastModifyingUserId: cHolder.apiClient.userId,
        lastModifyingMachineId: cHolder.settingsClient.currentSettings.machineId.valueOrDefault,
        lastModificationTime: DateTime.now().toUtc(),
        creationTime: DateTime.now().toUtc(),
        isSynced: false,
        fetchedOn: DateTime.now().toUtc(),
        combinedRecordId: RecordId(
          id: Record.generateId(),
          ownerId: cHolder.apiClient.userId,
          isValid: true,
        ),
      );

      // Save the record to inventory
      await RecordApi.upsertRecord(cHolder.apiClient, record: newRecord);
      
      // Reload the inventory to show the new item
      await inventoryClient.reloadCurrentDirectory();

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text("Saved to inventory")),
        );
      }
    } catch (e, s) {
      FlutterError.reportError(FlutterErrorDetails(exception: e, stack: s));
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text("Failed to save to inventory: $e")),
        );
      }
    } finally {
      if (mounted) {
        setState(() {
          _isSavingToInventory = false;
        });
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final imageUri = _getImageUri();
    final imageUrl = Aux.resdbToHttp(imageUri);
    
    return Scaffold(
      appBar: AppBar(
        title: Text(widget.formattedName.toString()),
        actions: [
          if (_isSaving || _isSavingToInventory)
            const Padding(
              padding: EdgeInsets.all(16.0),
              child: SizedBox(
                width: 20,
                height: 20,
                child: CircularProgressIndicator(strokeWidth: 2),
              ),
            )
          else ...[
            Builder(
              builder: (context) {
                try {
                  Provider.of<InventoryClient>(context, listen: false);
                  return IconButton(
                    icon: const Icon(Icons.inventory_2),
                    tooltip: "Save to inventory",
                    onPressed: _saveToInventory,
                  );
                } catch (_) {
                  // InventoryClient not available in this context
                  return const SizedBox.shrink();
                }
              },
            ),
            IconButton(
              icon: const Icon(Icons.download),
              tooltip: "Save to device",
              onPressed: _saveToDevice,
            ),
            IconButton(
              icon: const Icon(Icons.share),
              tooltip: "Share",
              onPressed: _shareImageDirect,
            ),
          ],
        ],
      ),
      body: PhotoView(
        minScale: PhotoViewComputedScale.contained,
        imageProvider: CachedNetworkImageProvider(imageUrl),
        heroAttributes: PhotoViewHeroAttributes(tag: widget.message.id),
      ),
    );
  }
}
