from __future__ import annotations

import os
import socket
from datetime import datetime

from typing import Dict, List
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi import Query, Path
from typing import Optional
from fastapi.responses import Response

from models.theatre import TheatreCreate, TheatreRead, TheatreUpdate
from models.screen import ScreenCreate, ScreenRead, ScreenUpdate
from models.movie import MovieCreate, MovieRead, MovieUpdate
from models.showtime import ShowtimeCreate, ShowtimeRead, ShowtimeUpdate
from models.health import Health
from models.theatreDataService import TheatreDataService

port = int(os.environ.get("FASTAPIPORT", 8001))

# -----------------------------------------------------------------------------
# Fake in-memory "databases"
# -----------------------------------------------------------------------------
theatres: Dict[UUID, TheatreRead] = {}
screens: Dict[UUID, ScreenRead] = {}
movies: Dict[UUID, MovieRead] = {}
showtimes: Dict[UUID, ShowtimeRead] = {}

app = FastAPI(
    title="Nebula Booking Theatre Service API",
    description="FastAPI app using Pydantic v2 models for Theatre, Screen, Movie, and Showtime management",
    version="0.1.0",
)

# -----------------------------------------------------------------------------
# Health endpoints
# -----------------------------------------------------------------------------

def make_health(echo: Optional[str], path_echo: Optional[str]=None) -> Health:
    return Health(
        status=200,
        status_message="OK",
        timestamp=datetime.utcnow().isoformat() + "Z",
        ip_address=socket.gethostbyname(socket.gethostname()),
        echo=echo,
        path_echo=path_echo
    )

@app.get("/health", response_model=Health)
def get_health_no_path(echo: str | None = Query(None, description="Optional echo string")):
    return make_health(echo=echo, path_echo=None)

@app.get("/health/{path_echo}", response_model=Health)
def get_health_with_path(
    path_echo: str = Path(..., description="Required echo in the URL path"),
    echo: str | None = Query(None, description="Optional echo string"),
):
    return make_health(echo=echo, path_echo=path_echo)

# -----------------------------------------------------------------------------
# Favicon (avoid noisy 404s from browsers)
# -----------------------------------------------------------------------------
@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)

# -----------------------------------------------------------------------------
# Theatre endpoints
# -----------------------------------------------------------------------------

@app.post("/theatres", response_model=TheatreRead, status_code=201)
def create_theatre(theatre: TheatreCreate):
    """Create a new theatre."""
    new_theatre = TheatreRead(**theatre.model_dump())
    theatres[new_theatre.id] = new_theatre
    return new_theatre

@app.get("/theatres", response_model=List[TheatreRead])
def list_theatres(
    name: Optional[str] = Query(None, description="Filter by theatre name"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    country: Optional[str] = Query(None, description="Filter by country"),
):
    """List all theatres with optional filtering."""
    items = TheatreDataService().get_all_theatres()
    ans=[]
    for item in items:
        obj = {}
        obj['id'] = "99999999-9999-4999-8999-999999999999"
        obj['name'] = item['name']
        obj['address'] = item['address']
        obj['city'] = ""
        obj['state'] = ""
        obj['postal_code'] = ""
        obj['country'] = ""
        obj['phone'] = ""
        obj['email'] = "a@b.com"
        obj['capacity'] = 60
        obj['created_at'] = item['created_at'].isoformat() + "Z"
        obj['updated_at'] = item['updated_at'].isoformat() + "Z"

        ans.append(obj)
    
    return ans

@app.get("/theatres/{theatre_id}", response_model=TheatreRead)
def get_theatre(theatre_id: UUID):
    """Get a specific theatre by ID."""
    item = theatres.get(theatre_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Theatre not found")
    return item

@app.patch("/theatres/{theatre_id}", response_model=TheatreRead)
def update_theatre(theatre_id: UUID, update: TheatreUpdate):
    """Update a theatre (partial update)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.put("/theatres/{theatre_id}", response_model=TheatreRead)
def replace_theatre(theatre_id: UUID, theatre: TheatreCreate):
    """Replace entire theatre resource (PUT - complete replacement)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.delete("/theatres/{theatre_id}")
def delete_theatre(theatre_id: UUID):
    """Delete a theatre resource."""
    if theatre_id not in theatres:
        raise HTTPException(status_code=404, detail="Theatre not found")
    del theatres[theatre_id]
    return {"status": "deleted", "id": str(theatre_id)}

# -----------------------------------------------------------------------------
# Screen endpoints
# -----------------------------------------------------------------------------

@app.post("/screens", response_model=ScreenRead, status_code=201)
def create_screen(screen: ScreenCreate):
    """Create a new screen."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/screens", response_model=List[ScreenRead])
def list_screens(
    theatre_id: Optional[UUID] = Query(None, description="Filter by theatre ID"),
    screen_number: Optional[int] = Query(None, description="Filter by screen number"),
    screen_type: Optional[str] = Query(None, description="Filter by screen type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
):
    """List all screens with optional filtering."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/screens/{screen_id}", response_model=ScreenRead)
def get_screen(screen_id: UUID):
    """Get a specific screen by ID."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.patch("/screens/{screen_id}", response_model=ScreenRead)
def update_screen(screen_id: UUID, update: ScreenUpdate):
    """Update a screen (partial update)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.put("/screens/{screen_id}", response_model=ScreenRead)
def replace_screen(screen_id: UUID, screen: ScreenCreate):
    """Replace entire screen resource (PUT - complete replacement)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.delete("/screens/{screen_id}")
def delete_screen(screen_id: UUID):
    """Delete a screen resource."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

# -----------------------------------------------------------------------------
# Movie endpoints
# -----------------------------------------------------------------------------

@app.post("/movies", response_model=MovieRead, status_code=201)
def create_movie(movie: MovieCreate):
    """Create a new movie."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/movies", response_model=List[MovieRead])
def list_movies(
    title: Optional[str] = Query(None, description="Filter by movie title"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    rating: Optional[str] = Query(None, description="Filter by rating"),
    director: Optional[str] = Query(None, description="Filter by director"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
):
    """List all movies with optional filtering."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/movies/{movie_id}", response_model=MovieRead)
def get_movie(movie_id: UUID):
    """Get a specific movie by ID."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.patch("/movies/{movie_id}", response_model=MovieRead)
def update_movie(movie_id: UUID, update: MovieUpdate):
    """Update a movie (partial update)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.put("/movies/{movie_id}", response_model=MovieRead)
def replace_movie(movie_id: UUID, movie: MovieCreate):
    """Replace entire movie resource (PUT - complete replacement)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.delete("/movies/{movie_id}")
def delete_movie(movie_id: UUID):
    """Delete a movie resource."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

# -----------------------------------------------------------------------------
# Showtime endpoints
# -----------------------------------------------------------------------------

@app.post("/showtimes", response_model=ShowtimeRead, status_code=201)
def create_showtime(showtime: ShowtimeCreate):
    """Create a new showtime."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/showtimes", response_model=List[ShowtimeRead])
def list_showtimes(
    theatre_id: Optional[UUID] = Query(None, description="Filter by theatre ID"),
    screen_id: Optional[UUID] = Query(None, description="Filter by screen ID"),
    movie_id: Optional[UUID] = Query(None, description="Filter by movie ID"),
    show_date: Optional[str] = Query(None, description="Filter by show date (YYYY-MM-DD)"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
):
    """List all showtimes with optional filtering."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.get("/showtimes/{showtime_id}", response_model=ShowtimeRead)
def get_showtime(showtime_id: UUID):
    """Get a specific showtime by ID."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.patch("/showtimes/{showtime_id}", response_model=ShowtimeRead)
def update_showtime(showtime_id: UUID, update: ShowtimeUpdate):
    """Update a showtime (partial update)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.put("/showtimes/{showtime_id}", response_model=ShowtimeRead)
def replace_showtime(showtime_id: UUID, showtime: ShowtimeCreate):
    """Replace entire showtime resource (PUT - complete replacement)."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

@app.delete("/showtimes/{showtime_id}")
def delete_showtime(showtime_id: UUID):
    """Delete a showtime resource."""
    return HTTPException(status_code=501, detail="NOT IMPLEMENTED")

# -----------------------------------------------------------------------------
# Root
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "Welcome to the Nebula Booking Theatre Service API. See /docs for OpenAPI UI."}

# -----------------------------------------------------------------------------
# Entrypoint for `python main.py`
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
