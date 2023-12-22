from pydantic import BaseModel
from fastapi import HTTPException, status


class HTTPError(BaseModel):
    detail: str
    status_code: int


class LayerError(Exception):
    """Base class for exceptions related to layers."""

    pass


class LayerSizeError(LayerError):
    """Raised when the layer size is not valid to continue the operation."""

    pass


class LayerExtentError(LayerError):
    """Raised when the layer extent is not valid to continue the operation."""

    pass


class LayerProjectTypeError(LayerError):
    """Raised when the layer_project is not of type BaseModel, SQLModel or dict."""

    pass


class FeatureCountError(LayerError):
    """Raised when the operation cannot be performed on more than a certain number of features."""

    pass


class GeometryTypeError(LayerError):
    """Raised when the operation requires another geometry type."""

    pass


class AreaSizeError(LayerError):
    """Raised when the operation cannot be performed on more than a certain area."""

    pass


class OutOfGeofenceError(LayerError):
    """Raised when the operation cannot be performed outside the geofence."""

    pass


class UnsupportedLayerTypeError(LayerError):
    """Raised when the layer type is not supported."""

    pass


class ColumnTypeError(LayerError):
    """Raised when the column type is not supported."""

    pass


class LayerNotFoundError(LayerError):
    """Raised when the layer is not found."""

    pass

class SQLError(Exception):
    """Base class for exceptions related to SQL."""

    pass


# Define the mapping between custom errors and HTTP status codes
ERROR_MAPPING = {
    LayerSizeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    LayerExtentError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    LayerProjectTypeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    FeatureCountError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    GeometryTypeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    AreaSizeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    OutOfGeofenceError: status.HTTP_403_FORBIDDEN,
    UnsupportedLayerTypeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    ColumnTypeError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    LayerNotFoundError: status.HTTP_404_NOT_FOUND,
    SQLError: status.HTTP_500_INTERNAL_SERVER_ERROR,
}


async def http_error_handler(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        error_status_code = ERROR_MAPPING.get(type(e))
        if error_status_code:
            raise HTTPException(status_code=error_status_code, detail=str(e))
        else:
            # Raise generic HTTP error
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
            )
