"""Showtime API endpoints - connected to database."""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends, Header
from fastapi.responses import Response
import requests

from config import Config

from schemas.showtime import (
    ShowtimeCreate,
    ShowtimeRead,
    ShowtimeUpdate,
    SeatUpdateRequest,
    SeatAvailabilityResponse
)
from services.showtimeDataService import ShowtimeDataService
from services.screenDataService import ScreenDataService
from database import get_db
from sqlalchemy.orm import Session
from utils.etag import calc_etag
from utils.converters import dict_to_showtime_read


router = APIRouter(prefix="/showtimes", tags=["showtimes"])


@router.post("", response_model=ShowtimeRead, status_code=201)
def create_showtime(showtime: ShowtimeCreate, response: Response, db: Session = Depends(get_db)):
    """Create a new showtime in the database."""
    db_service = ShowtimeDataService()
    screen_service = ScreenDataService()
    
    # Verify screen exists
    screen_id_int = showtime.screen_id
    screen = screen_service.get_screen_by_id(db, screen_id_int)
    if not screen:
        raise HTTPException(status_code=404, detail=f"Screen {screen_id_int} not found")
    
    # Verify movie exists by calling Movie Service
    movie_service_url = Config.MOVIE_SERVICE_URL.rstrip("/")
    movie_check_url = f"{movie_service_url}/movies/{showtime.movie_id}"
    try:
        mv_resp = requests.get(movie_check_url, timeout=5)
    except requests.RequestException as exc:
        # Upstream service unavailable or network error
        raise HTTPException(status_code=502, detail=f"Movie service unavailable: {exc}")

    if mv_resp.status_code == 404:
        # Forward the upstream 404 body if possible
        try:
            payload = mv_resp.json()
        except Exception:
            payload = {"detail": f"Movie {showtime.movie_id} not found"}
        raise HTTPException(status_code=404, detail=payload)
    elif mv_resp.status_code >= 400:
        # For other client/server errors from movie service, propagate a 502
        try:
            payload = mv_resp.json()
        except Exception:
            payload = {"detail": f"Upstream error: {mv_resp.status_code}"}
        raise HTTPException(status_code=502, detail=payload)

    # All checks passed; create showtime
    db_item = db_service.create_showtime(
        db=db,
        screen_id=screen_id_int,
        movie_id=showtime.movie_id,
        start_time=showtime.start_time,
        seats_booked=showtime.seats_booked,
        price=showtime.price,
        created_by=1,  # Placeholder - would come from auth
    )
    
    # db_item = db_service.get_showtime_by_id(showtime_id)
    # if not db_item:
    #     raise HTTPException(status_code=500, detail="Failed to create showtime")
    
    new_showtime = ShowtimeRead(**dict_to_showtime_read(db_item))
    response.headers["ETag"] = calc_etag(new_showtime)
    return new_showtime


@router.get("", response_model=List[ShowtimeRead])
def list_showtimes(
    screen_id: Optional[int] = Query(None, description="Filter by screen ID"),
    movie_id: Optional[int] = Query(None, description="Filter by movie ID"),
    start_time_after: Optional[datetime] = Query(None, description="Filter showtimes starting after this time"),
    db: Session = Depends(get_db),
):
    """List all showtimes from the database with optional filtering."""
    db_service = ShowtimeDataService()
    
    # Get all showtimes or filter by specific criteria
    if screen_id:
        db_showtimes = db_service.get_showtimes_by_screen(db, screen_id)
    elif movie_id is not None:
        db_showtimes = db_service.get_showtimes_by_movie(db, movie_id)
    else:
        db_showtimes = db_service.get_all_showtimes(db)
    
    items: List[ShowtimeRead] = [
        ShowtimeRead(**dict_to_showtime_read(db_item))
        for db_item in db_showtimes
    ]
    
    # Apply additional filters
    if start_time_after:
        items = [s for s in items if s.start_time >= start_time_after]
    
    return items

@router.get("/{showtime_id}", response_model=ShowtimeRead)
def get_showtime(
    showtime_id: int,
    response: Response,
    if_none_match: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Get a specific showtime from the database by ID."""
    db_service = ShowtimeDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    item = ShowtimeRead(**dict_to_showtime_read(db_item))
    etag = calc_etag(item)
    
    if if_none_match == etag:
        return Response(status_code=304, headers={"ETag": etag})
    
    response.headers["ETag"] = etag
    return item


@router.patch("/{showtime_id}", response_model=ShowtimeRead)
def update_showtime(
    showtime_id: int,
    update: ShowtimeUpdate,
    response: Response,
    if_match: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Update a showtime (partial update) in the database."""
    db_service = ShowtimeDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    existing = ShowtimeRead(**dict_to_showtime_read(db_item))
    current_etag = calc_etag(existing)
    
    if if_match is None:
        raise HTTPException(status_code=428, detail="Precondition Required: missing If-Match")
    if if_match != current_etag:
        raise HTTPException(status_code=412, detail="Precondition Failed: ETag mismatch")
    
    updates = update.model_dump(exclude_none=True)
    success = db_service.update_showtime(
        db=db,
        showtime_id=showtime_id,
        movie_id=updates.get('movie_id'),
        start_time=updates.get('start_time'),
        seats_booked=updates.get('seats_booked'),
        price=updates.get('price')
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update showtime")
    
    db_item = db_service.get_showtime_by_id(showtime_id_int)
    updated = ShowtimeRead(**dict_to_showtime_read(db_item))
    
    new_etag = calc_etag(updated)
    response.headers["ETag"] = new_etag
    return updated


@router.put("/{showtime_id}", response_model=ShowtimeRead)
def replace_showtime(
    showtime_id: int,
    showtime: ShowtimeCreate,
    response: Response,
    if_match: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Replace entire showtime resource (PUT) in the database."""
    db_service = ShowtimeDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    existing = ShowtimeRead(**dict_to_showtime_read(db_item))
    current_etag = calc_etag(existing)
    
    if if_match is None:
        raise HTTPException(status_code=428, detail="Precondition Required: missing If-Match")
    if if_match != current_etag:
        raise HTTPException(status_code=412, detail="Precondition Failed: ETag mismatch")
    
    success = db_service.update_showtime(
        db=db,
        showtime_id=showtime_id,
        movie_id=showtime.movie_id,
        start_time=showtime.start_time,
        seats_booked=showtime.seats_booked,
        price=showtime.price
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to replace showtime")
    
    db_item = db_service.get_showtime_by_id(showtime_id_int)
    replacement = ShowtimeRead(**dict_to_showtime_read(db_item))
    
    new_etag = calc_etag(replacement)
    response.headers["ETag"] = new_etag
    return replacement


@router.delete("/{showtime_id}")
def delete_showtime(
    showtime_id: int,
    if_match: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Soft delete a showtime from the database."""
    db_service = ShowtimeDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    existing = ShowtimeRead(**dict_to_showtime_read(db_item))
    current_etag = calc_etag(existing)
    
    if if_match is not None and if_match != current_etag:
        raise HTTPException(status_code=412, detail="Precondition Failed: ETag mismatch")
    
    success = db_service.delete_showtime(db, showtime_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete showtime")
    
    return {"status": "deleted", "id": str(showtime_id)}


@router.get("/{showtime_id}/availability", response_model=SeatAvailabilityResponse)
def get_seat_availability(showtime_id: int, db: Session = Depends(get_db)):
    """Get seat availability information for a showtime from the database."""
    db_service = ShowtimeDataService()
    screen_service = ScreenDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    showtime = ShowtimeRead(**dict_to_showtime_read(db_item))
    
    # Get screen information
    screen_id_int = showtime.screen_id
    screen_data = screen_service.get_screen_by_id(db, screen_id_int)
    if not screen_data:
        raise HTTPException(status_code=404, detail="Screen not found for this showtime")
    
    total_seats = screen_data['num_rows'] * screen_data['num_cols']
    seats_available = total_seats - showtime.seats_booked
    
    return SeatAvailabilityResponse(
        showtime_id=showtime_id,
        screen_id=showtime.screen_id,
        total_seats=total_seats,
        seats_booked=showtime.seats_booked,
        seats_available=seats_available
    )


@router.post("/{showtime_id}/seats", response_model=ShowtimeRead)
def update_seat_count(
    showtime_id: int,
    seat_update: SeatUpdateRequest,
    response: Response,
    db: Session = Depends(get_db)
):
    """
    Update the seat count for a showtime in the database.
    Positive count = book seats, Negative count = release seats.
    Called by Booking Service when seats are booked or released.
    """
    db_service = ShowtimeDataService()
    screen_service = ScreenDataService()
    showtime_id_int = int(str(showtime_id).replace('-', '')[:8], 16) % (10**9)
    
    db_item = db_service.get_showtime_by_id(db, showtime_id)
    if not db_item:
        raise HTTPException(status_code=404, detail="Showtime not found")
    
    showtime = ShowtimeRead(**dict_to_showtime_read(db_item))
    
    # Get screen to validate seat count
    screen_id_int = showtime.screen_id
    screen_data = screen_service.get_screen_by_id(db, screen_id_int)
    if not screen_data:
        raise HTTPException(status_code=404, detail="Screen not found for this showtime")
    
    total_seats = screen_data.num_rows * screen_data.num_cols
    new_booked_count = showtime.seats_booked + seat_update.count
    
    # Validate seat count
    if new_booked_count < 0:
        raise HTTPException(
            status_code=400,
            detail="Cannot release more seats than are currently booked"
        )
    if new_booked_count > total_seats:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot book more seats than available. Total seats: {total_seats}, Already booked: {showtime.seats_booked}"
        )
    
    # Update seat count in database
    updated_item = db_service.update_seat_count(db, showtime_id, seat_update.count)
    if not updated_item:
        raise HTTPException(status_code=500, detail="Failed to update seat count")
    
    updated = ShowtimeRead(**dict_to_showtime_read(updated_item))
    etag = calc_etag(updated)
    response.headers["ETag"] = etag
    return updated
