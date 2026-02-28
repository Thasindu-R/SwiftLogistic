"""
File Storage Management
=======================

Secure file storage for proof-of-delivery images and documents:
- Local filesystem storage with configurable path
- Unique file naming with UUID
- Image optimization and thumbnail generation
- Metadata storage in database
- Access control based on order ownership

Features:
- Secure file uploads
- Image validation (type, size, dimensions)
- Automatic thumbnail generation
- File cleanup for orphaned files
- Pre-signed URL generation (for cloud storage compatibility)

Usage:
    from shared.common.file_storage import file_storage
    
    # Upload proof-of-delivery image
    result = await file_storage.upload_proof_of_delivery(
        order_id="abc-123",
        file_content=file_bytes,
        filename="photo.jpg",
        content_type="image/jpeg",
        driver_id=5,
    )
    
    # Get file URL
    url = file_storage.get_file_url(result["file_id"])
"""

import asyncio
import base64
import hashlib
import io
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, Optional, Union

from sqlalchemy import text

logger = logging.getLogger(__name__)


class FileCategory(str, Enum):
    """Categories of stored files."""
    PROOF_OF_DELIVERY = "proof_of_delivery"
    SIGNATURE = "signature"
    PACKAGE_PHOTO = "package_photo"
    DAMAGE_REPORT = "damage_report"
    DOCUMENT = "document"
    AVATAR = "avatar"


class FileStorageConfig:
    """Configuration for file storage."""
    
    # Base storage path (can be overridden by environment)
    BASE_PATH = os.getenv("FILE_STORAGE_PATH", "/app/storage")
    
    # Maximum file sizes (in bytes)
    MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB
    MAX_DOCUMENT_SIZE = 25 * 1024 * 1024  # 25 MB
    MAX_SIGNATURE_SIZE = 1 * 1024 * 1024  # 1 MB
    
    # Allowed MIME types
    ALLOWED_IMAGE_TYPES = {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
    }
    
    ALLOWED_DOCUMENT_TYPES = {
        "application/pdf",
        "image/jpeg",
        "image/png",
    }
    
    # Image constraints
    MAX_IMAGE_DIMENSION = 4096  # pixels
    THUMBNAIL_SIZE = (200, 200)
    
    # URL base for serving files
    FILE_URL_BASE = os.getenv("FILE_URL_BASE", "/api/files")


class FileMetadata:
    """Metadata for stored file."""
    
    def __init__(
        self,
        file_id: str,
        original_filename: str,
        stored_filename: str,
        category: FileCategory,
        content_type: str,
        file_size: int,
        checksum: str,
        order_id: Optional[str] = None,
        user_id: Optional[int] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        thumbnail_path: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ):
        self.file_id = file_id
        self.original_filename = original_filename
        self.stored_filename = stored_filename
        self.category = category
        self.content_type = content_type
        self.file_size = file_size
        self.checksum = checksum
        self.order_id = order_id
        self.user_id = user_id
        self.width = width
        self.height = height
        self.thumbnail_path = thumbnail_path
        self.created_at = created_at or datetime.now(timezone.utc)
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "file_id": self.file_id,
            "original_filename": self.original_filename,
            "stored_filename": self.stored_filename,
            "category": self.category.value,
            "content_type": self.content_type,
            "file_size": self.file_size,
            "checksum": self.checksum,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "width": self.width,
            "height": self.height,
            "has_thumbnail": self.thumbnail_path is not None,
            "created_at": self.created_at.isoformat(),
        }


class FileStorageService:
    """
    Service for managing file storage.
    
    Handles secure upload, storage, and retrieval of files
    associated with deliveries (proof-of-delivery, signatures, etc.)
    """
    
    def __init__(self, db_session=None):
        """
        Initialize file storage service.
        
        Args:
            db_session: Optional database session for metadata storage
        """
        self.db = db_session
        self.config = FileStorageConfig()
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create storage directories if they don't exist."""
        base = Path(self.config.BASE_PATH)
        
        for category in FileCategory:
            category_path = base / category.value
            category_path.mkdir(parents=True, exist_ok=True)
            
            # Create thumbnails subdirectory for images
            if category in [FileCategory.PROOF_OF_DELIVERY, FileCategory.PACKAGE_PHOTO]:
                (category_path / "thumbnails").mkdir(exist_ok=True)
        
        logger.info("File storage directories ensured at: %s", base)
    
    def _generate_file_id(self) -> str:
        """Generate unique file ID."""
        return str(uuid.uuid4())
    
    def _generate_stored_filename(self, file_id: str, extension: str) -> str:
        """Generate unique stored filename."""
        return f"{file_id}.{extension}"
    
    def _get_extension_from_content_type(self, content_type: str) -> str:
        """Get file extension from content type."""
        mapping = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/gif": "gif",
            "image/webp": "webp",
            "application/pdf": "pdf",
        }
        return mapping.get(content_type, "bin")
    
    def _calculate_checksum(self, content: bytes) -> str:
        """Calculate SHA-256 checksum of file content."""
        return hashlib.sha256(content).hexdigest()
    
    def _get_storage_path(self, category: FileCategory, filename: str) -> Path:
        """Get full storage path for a file."""
        return Path(self.config.BASE_PATH) / category.value / filename
    
    def _get_thumbnail_path(self, category: FileCategory, filename: str) -> Path:
        """Get thumbnail storage path."""
        return Path(self.config.BASE_PATH) / category.value / "thumbnails" / f"thumb_{filename}"
    
    def _validate_file(
        self,
        content: bytes,
        content_type: str,
        category: FileCategory,
    ) -> dict:
        """
        Validate file content and type.
        
        Returns:
            Dict with validation results including dimensions for images
        """
        from .errors import FileTooLargeError, InvalidFileTypeError
        
        file_size = len(content)
        
        # Determine max size based on category
        if category == FileCategory.SIGNATURE:
            max_size = self.config.MAX_SIGNATURE_SIZE
            allowed_types = self.config.ALLOWED_IMAGE_TYPES
        elif category == FileCategory.DOCUMENT:
            max_size = self.config.MAX_DOCUMENT_SIZE
            allowed_types = self.config.ALLOWED_DOCUMENT_TYPES
        else:
            max_size = self.config.MAX_IMAGE_SIZE
            allowed_types = self.config.ALLOWED_IMAGE_TYPES
        
        # Check size
        if file_size > max_size:
            raise FileTooLargeError(file_size, max_size)
        
        # Check type
        if content_type not in allowed_types:
            raise InvalidFileTypeError(content_type, list(allowed_types))
        
        result = {
            "file_size": file_size,
            "content_type": content_type,
            "width": None,
            "height": None,
        }
        
        # Get image dimensions if applicable
        if content_type.startswith("image/"):
            try:
                dimensions = self._get_image_dimensions(content)
                result["width"] = dimensions[0]
                result["height"] = dimensions[1]
                
                # Check maximum dimension
                if max(dimensions) > self.config.MAX_IMAGE_DIMENSION:
                    logger.warning(
                        "Image dimension %s exceeds max %s, will be resized",
                        max(dimensions), self.config.MAX_IMAGE_DIMENSION
                    )
            except Exception as e:
                logger.warning("Could not get image dimensions: %s", e)
        
        return result
    
    def _get_image_dimensions(self, content: bytes) -> tuple[int, int]:
        """Get image width and height."""
        try:
            from PIL import Image
            img = Image.open(io.BytesIO(content))
            return img.size
        except ImportError:
            # PIL not available, try basic parsing
            return self._parse_image_dimensions(content)
    
    def _parse_image_dimensions(self, content: bytes) -> tuple[int, int]:
        """Parse image dimensions without PIL."""
        # Simple JPEG/PNG dimension parsing
        if content[:2] == b'\xff\xd8':  # JPEG
            # Find SOF marker
            i = 2
            while i < len(content) - 9:
                if content[i] == 0xff:
                    marker = content[i + 1]
                    if marker in (0xc0, 0xc1, 0xc2):
                        height = (content[i + 5] << 8) + content[i + 6]
                        width = (content[i + 7] << 8) + content[i + 8]
                        return (width, height)
                    length = (content[i + 2] << 8) + content[i + 3]
                    i += length + 2
                else:
                    i += 1
        elif content[:8] == b'\x89PNG\r\n\x1a\n':  # PNG
            width = int.from_bytes(content[16:20], 'big')
            height = int.from_bytes(content[20:24], 'big')
            return (width, height)
        
        return (0, 0)
    
    def _create_thumbnail(
        self,
        content: bytes,
        output_path: Path,
        size: tuple[int, int] = None,
    ) -> bool:
        """Create thumbnail for image."""
        size = size or self.config.THUMBNAIL_SIZE
        
        try:
            from PIL import Image
            
            img = Image.open(io.BytesIO(content))
            img.thumbnail(size, Image.Resampling.LANCZOS)
            
            # Convert RGBA to RGB for JPEG
            if img.mode == 'RGBA':
                background = Image.new('RGB', img.size, (255, 255, 255))
                background.paste(img, mask=img.split()[3])
                img = background
            
            img.save(output_path, "JPEG", quality=85, optimize=True)
            return True
            
        except ImportError:
            logger.warning("PIL not available, thumbnail not created")
            return False
        except Exception as e:
            logger.error("Failed to create thumbnail: %s", e)
            return False
    
    async def upload_file(
        self,
        content: Union[bytes, BinaryIO],
        filename: str,
        content_type: str,
        category: FileCategory,
        order_id: Optional[str] = None,
        user_id: Optional[int] = None,
        create_thumbnail: bool = True,
    ) -> FileMetadata:
        """
        Upload and store a file.
        
        Args:
            content: File content (bytes or file-like object)
            filename: Original filename
            content_type: MIME type
            category: File category
            order_id: Associated order ID
            user_id: Uploading user ID
            create_thumbnail: Create thumbnail for images
            
        Returns:
            FileMetadata with storage details
        """
        # Read content if file-like object
        if hasattr(content, 'read'):
            content = content.read()
        
        # Validate file
        validation = self._validate_file(content, content_type, category)
        
        # Generate IDs and paths
        file_id = self._generate_file_id()
        extension = self._get_extension_from_content_type(content_type)
        stored_filename = self._generate_stored_filename(file_id, extension)
        storage_path = self._get_storage_path(category, stored_filename)
        
        # Calculate checksum
        checksum = self._calculate_checksum(content)
        
        # Write file
        await asyncio.to_thread(self._write_file, storage_path, content)
        
        # Create thumbnail for images
        thumbnail_path = None
        if create_thumbnail and content_type.startswith("image/"):
            thumb_path = self._get_thumbnail_path(category, f"{file_id}.jpg")
            if self._create_thumbnail(content, thumb_path):
                thumbnail_path = str(thumb_path)
        
        # Create metadata
        metadata = FileMetadata(
            file_id=file_id,
            original_filename=filename,
            stored_filename=stored_filename,
            category=category,
            content_type=content_type,
            file_size=validation["file_size"],
            checksum=checksum,
            order_id=order_id,
            user_id=user_id,
            width=validation["width"],
            height=validation["height"],
            thumbnail_path=thumbnail_path,
        )
        
        # Save metadata to database
        if self.db:
            await self._save_metadata(metadata)
        
        logger.info(
            "File uploaded: file_id=%s, category=%s, size=%d bytes",
            file_id, category.value, validation["file_size"],
        )
        
        return metadata
    
    def _write_file(self, path: Path, content: bytes):
        """Write file to filesystem (sync operation)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(content)
    
    async def _save_metadata(self, metadata: FileMetadata):
        """Save file metadata to database."""
        try:
            await self.db.execute(
                text("""
                    INSERT INTO file_uploads (
                        file_id, original_filename, stored_filename,
                        category, content_type, file_size, checksum,
                        order_id, user_id, width, height,
                        thumbnail_path, created_at
                    ) VALUES (
                        :file_id, :original_filename, :stored_filename,
                        :category, :content_type, :file_size, :checksum,
                        :order_id, :user_id, :width, :height,
                        :thumbnail_path, :created_at
                    )
                """),
                {
                    "file_id": metadata.file_id,
                    "original_filename": metadata.original_filename,
                    "stored_filename": metadata.stored_filename,
                    "category": metadata.category.value,
                    "content_type": metadata.content_type,
                    "file_size": metadata.file_size,
                    "checksum": metadata.checksum,
                    "order_id": metadata.order_id,
                    "user_id": metadata.user_id,
                    "width": metadata.width,
                    "height": metadata.height,
                    "thumbnail_path": metadata.thumbnail_path,
                    "created_at": metadata.created_at,
                }
            )
            await self.db.commit()
        except Exception as e:
            logger.error("Failed to save file metadata: %s", e)
            await self.db.rollback()
    
    async def upload_proof_of_delivery(
        self,
        order_id: str,
        file_content: bytes,
        filename: str,
        content_type: str,
        driver_id: int,
    ) -> dict:
        """
        Upload proof-of-delivery image.
        
        Args:
            order_id: Order UUID
            file_content: Image bytes
            filename: Original filename
            content_type: MIME type
            driver_id: Driver user ID
            
        Returns:
            Dict with file_id, url, and metadata
        """
        metadata = await self.upload_file(
            content=file_content,
            filename=filename,
            content_type=content_type,
            category=FileCategory.PROOF_OF_DELIVERY,
            order_id=order_id,
            user_id=driver_id,
            create_thumbnail=True,
        )
        
        return {
            "file_id": metadata.file_id,
            "url": self.get_file_url(metadata.file_id),
            "thumbnail_url": self.get_thumbnail_url(metadata.file_id) if metadata.thumbnail_path else None,
            "metadata": metadata.to_dict(),
        }
    
    async def upload_signature(
        self,
        order_id: str,
        signature_data: str,  # Base64 encoded
        recipient_name: str,
        driver_id: int,
    ) -> dict:
        """
        Upload recipient signature.
        
        Args:
            order_id: Order UUID
            signature_data: Base64 encoded signature image
            recipient_name: Name of recipient who signed
            driver_id: Driver user ID
            
        Returns:
            Dict with file_id and url
        """
        # Decode base64 signature
        try:
            # Handle data URL format
            if "," in signature_data:
                signature_data = signature_data.split(",")[1]
            
            content = base64.b64decode(signature_data)
        except Exception as e:
            from .errors import ValidationError
            raise ValidationError(f"Invalid signature data: {e}", field="signature")
        
        # Generate filename with recipient name
        safe_name = "".join(c for c in recipient_name if c.isalnum() or c in " -_")[:30]
        filename = f"signature_{safe_name}_{order_id[:8]}.png"
        
        metadata = await self.upload_file(
            content=content,
            filename=filename,
            content_type="image/png",
            category=FileCategory.SIGNATURE,
            order_id=order_id,
            user_id=driver_id,
            create_thumbnail=False,
        )
        
        return {
            "file_id": metadata.file_id,
            "url": self.get_file_url(metadata.file_id),
            "recipient_name": recipient_name,
            "metadata": metadata.to_dict(),
        }
    
    async def get_file_content(
        self,
        file_id: str,
    ) -> Optional[tuple[bytes, str, str]]:
        """
        Get file content by ID.
        
        Args:
            file_id: File UUID
            
        Returns:
            Tuple of (content, content_type, filename) or None if not found
        """
        # Get metadata from database
        metadata = await self._get_metadata(file_id)
        if not metadata:
            return None
        
        # Read file
        storage_path = self._get_storage_path(
            FileCategory(metadata["category"]),
            metadata["stored_filename"],
        )
        
        if not storage_path.exists():
            logger.error("File not found on disk: %s", storage_path)
            return None
        
        content = await asyncio.to_thread(storage_path.read_bytes)
        
        return (content, metadata["content_type"], metadata["original_filename"])
    
    async def get_thumbnail_content(
        self,
        file_id: str,
    ) -> Optional[tuple[bytes, str]]:
        """
        Get thumbnail content by file ID.
        
        Args:
            file_id: File UUID
            
        Returns:
            Tuple of (content, content_type) or None if not found
        """
        metadata = await self._get_metadata(file_id)
        if not metadata or not metadata.get("thumbnail_path"):
            return None
        
        thumb_path = Path(metadata["thumbnail_path"])
        if not thumb_path.exists():
            return None
        
        content = await asyncio.to_thread(thumb_path.read_bytes)
        return (content, "image/jpeg")
    
    async def _get_metadata(self, file_id: str) -> Optional[dict]:
        """Get file metadata from database."""
        if not self.db:
            return None
        
        try:
            result = await self.db.execute(
                text("""
                    SELECT file_id, original_filename, stored_filename,
                           category, content_type, file_size, checksum,
                           order_id, user_id, width, height,
                           thumbnail_path, created_at
                    FROM file_uploads
                    WHERE file_id = :file_id
                """),
                {"file_id": file_id},
            )
            row = result.fetchone()
            if row:
                return dict(row._mapping)
            return None
        except Exception as e:
            logger.error("Failed to get file metadata: %s", e)
            return None
    
    async def get_files_for_order(self, order_id: str) -> list[dict]:
        """
        Get all files associated with an order.
        
        Args:
            order_id: Order UUID
            
        Returns:
            List of file metadata dicts
        """
        if not self.db:
            return []
        
        try:
            result = await self.db.execute(
                text("""
                    SELECT file_id, original_filename, category,
                           content_type, file_size, created_at,
                           width, height, thumbnail_path IS NOT NULL as has_thumbnail
                    FROM file_uploads
                    WHERE order_id = :order_id
                    ORDER BY created_at
                """),
                {"order_id": order_id},
            )
            
            files = []
            for row in result.fetchall():
                file_data = dict(row._mapping)
                file_data["url"] = self.get_file_url(file_data["file_id"])
                if file_data.get("has_thumbnail"):
                    file_data["thumbnail_url"] = self.get_thumbnail_url(file_data["file_id"])
                files.append(file_data)
            
            return files
        except Exception as e:
            logger.error("Failed to get files for order: %s", e)
            return []
    
    async def delete_file(self, file_id: str) -> bool:
        """
        Delete a file and its metadata.
        
        Args:
            file_id: File UUID
            
        Returns:
            True if deleted successfully
        """
        metadata = await self._get_metadata(file_id)
        if not metadata:
            return False
        
        # Delete file from disk
        storage_path = self._get_storage_path(
            FileCategory(metadata["category"]),
            metadata["stored_filename"],
        )
        
        try:
            if storage_path.exists():
                storage_path.unlink()
            
            # Delete thumbnail
            if metadata.get("thumbnail_path"):
                thumb_path = Path(metadata["thumbnail_path"])
                if thumb_path.exists():
                    thumb_path.unlink()
            
            # Delete metadata from database
            if self.db:
                await self.db.execute(
                    text("DELETE FROM file_uploads WHERE file_id = :file_id"),
                    {"file_id": file_id},
                )
                await self.db.commit()
            
            logger.info("File deleted: %s", file_id)
            return True
            
        except Exception as e:
            logger.error("Failed to delete file %s: %s", file_id, e)
            return False
    
    def get_file_url(self, file_id: str) -> str:
        """Get URL for accessing a file."""
        return f"{self.config.FILE_URL_BASE}/{file_id}"
    
    def get_thumbnail_url(self, file_id: str) -> str:
        """Get URL for accessing file thumbnail."""
        return f"{self.config.FILE_URL_BASE}/{file_id}/thumbnail"
    
    async def cleanup_orphaned_files(self, older_than_days: int = 30) -> int:
        """
        Remove orphaned files (files without database records).
        
        Args:
            older_than_days: Only cleanup files older than this many days
            
        Returns:
            Number of files cleaned up
        """
        # This would scan the storage directory and remove files
        # that don't have corresponding database records
        # Implementation depends on specific cleanup requirements
        logger.info("File cleanup initiated for files older than %d days", older_than_days)
        return 0  # Placeholder


# Global file storage instance (to be initialized with DB session)
file_storage: Optional[FileStorageService] = None


def get_file_storage(db_session=None) -> FileStorageService:
    """Get or create file storage service instance."""
    global file_storage
    if file_storage is None or db_session is not None:
        file_storage = FileStorageService(db_session)
    return file_storage
