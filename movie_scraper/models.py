"""
Data models for the movie scraper application.

Defines Pydantic models for films, screening sessions, and published events
with proper validation, serialization, and type safety.
"""

from datetime import date
from datetime import datetime
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import HttpUrl


class Film(BaseModel):
    """
    Represents a film with all metadata extracted from cinema listings.
    
    This model stores comprehensive film information including production
    details, ratings, and screening information. Used for database storage
    and API serialization.
    """
    
    id: Optional[int] = Field(
        default=None,
        description="Database primary key (auto-generated)"
    )
    
    slug: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Unique identifier extracted from afisha.ru URL"
    )
    
    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Film title in Russian"
    )
    
    country: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Country of production (used to filter non-Russian films)"
    )
    
    rating: Optional[str] = Field(
        default=None,
        max_length=20,
        description="Film rating (IMDb, Kinopoisk, etc.)"
    )
    
    description: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Film plot summary or description"
    )
    
    poster_url: Optional[HttpUrl] = Field(
        default=None,
        description="URL to film poster image"
    )
    
    age_limit: Optional[str] = Field(
        default=None,
        max_length=10,
        description="Age restriction (e.g., '12+', '16+', '18+')"
    )
    
    source_url: Optional[HttpUrl] = Field(
        default=None,
        description="Original afisha.ru page URL for this film"
    )
    
    last_seen: Optional[datetime] = Field(
        default=None,
        description="Last time this film was detected during scraping"
    )
    
    created_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when film was first added to database"
    )
    
    # Runtime metadata for processing
    next_screening: Optional[datetime] = Field(
        default=None,
        description="Next upcoming screening session (calculated)"
    )
    
    cinema_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of cinemas currently showing this film"
    )
    
    @field_validator("country")
    @classmethod
    def validate_country(cls, v: str) -> str:
        """Normalize and validate country name."""
        # Normalize country names for consistent filtering
        country_mappings = {
            "россия": "Россия",
            "russia": "Россия", 
            "рф": "Россия",
            "ussr": "СССР",
            "ссср": "СССР",
            "usa": "США",
            "сша": "США",
            "uk": "Великобритания",
            "великобритания": "Великобритания",
        }
        
        normalized = v.lower().strip()
        return country_mappings.get(normalized, v.title())
    
    @field_validator("age_limit")
    @classmethod
    def validate_age_limit(cls, v: Optional[str]) -> Optional[str]:
        """Validate age limit format."""
        if v is None:
            return v
        
        # Normalize age limit format
        cleaned = v.strip().lower()
        if cleaned in ["0+", "без ограничений", "all ages"]:
            return "0+"
        elif cleaned in ["6+", "6"]:
            return "6+"
        elif cleaned in ["12+", "12"]:
            return "12+"
        elif cleaned in ["16+", "16"]:
            return "16+"
        elif cleaned in ["18+", "18"]:
            return "18+"
        else:
            return v  # Return original if no match
    
    @property
    def is_foreign(self) -> bool:
        """Check if film is foreign (non-Russian)."""
        russian_countries = {"Россия", "СССР", "Russia", "РФ"}
        return self.country not in russian_countries
    
    @property
    def display_title(self) -> str:
        """Get display-friendly title with age limit."""
        if self.age_limit:
            return f"{self.title} ({self.age_limit})"
        return self.title
    
    def model_dump_calendar(self) -> dict:
        """Export film data optimized for ICS calendar generation."""
        return {
            "title": self.display_title,
            "description": self._build_calendar_description(),
            "url": str(self.source_url) if self.source_url else None,
            "slug": self.slug,
            "country": self.country,
            "next_screening": self.next_screening,
        }
    
    def _build_calendar_description(self) -> str:
        """Build rich description for calendar event."""
        parts = []
        
        if self.country:
            parts.append(f"Country: {self.country}")
        
        if self.rating:
            parts.append(f"Rating: {self.rating}")
        
        if self.description:
            # Truncate long descriptions for calendar
            desc = self.description[:300]
            if len(self.description) > 300:
                desc += "..."
            parts.append(f"\nPlot: {desc}")
        
        if self.poster_url:
            parts.append(f"\nPoster: {self.poster_url}")
        
        if self.source_url:
            parts.append(f"\nMore info: {self.source_url}")
        
        return "\n".join(parts)


class ScreeningSession(BaseModel):
    """
    Represents a single screening session at a cinema.
    
    Used to track when and where films are being shown, enabling
    calculation of next upcoming screenings for calendar events.
    """
    
    id: Optional[int] = Field(
        default=None,
        description="Database primary key (auto-generated)"
    )
    
    film_slug: str = Field(
        ...,
        min_length=1,
        description="Reference to associated film"
    )
    
    cinema_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Name of cinema showing the film"
    )
    
    session_datetime: datetime = Field(
        ...,
        description="Exact date and time of screening session"
    )
    
    format_info: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Screening format (2D, 3D, IMAX, etc.)"
    )
    
    language: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Language/subtitle information"
    )
    
    price_range: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Ticket price range if available"
    )
    
    last_updated: Optional[datetime] = Field(
        default=None,
        description="When this session data was last updated"
    )
    
    @field_validator("session_datetime")
    @classmethod
    def validate_future_session(cls, v: datetime) -> datetime:
        """Validate that session is in the future."""
        if v < datetime.now():
            # Log warning but don't reject - historical data can be useful
            pass
        return v
    
    @property
    def is_upcoming(self) -> bool:
        """Check if session is in the future."""
        return self.session_datetime > datetime.now()
    
    @property
    def date_only(self) -> date:
        """Get just the date portion for grouping."""
        return self.session_datetime.date()


class PublishedEvent(BaseModel):
    """
    Tracks calendar events that have been published to prevent duplicates.
    
    Implements the 30-day deduplication rule: once a film event is published
    for a specific date, don't create another event for the same film within
    30 days.
    """
    
    id: Optional[int] = Field(
        default=None,
        description="Database primary key (auto-generated)"
    )
    
    film_slug: str = Field(
        ...,
        min_length=1,
        description="Reference to associated film"
    )
    
    event_date: date = Field(
        ...,
        description="Date of the calendar event (all-day event)"
    )
    
    published_at: datetime = Field(
        ...,
        description="When this event was first published to calendar"
    )
    
    @property
    def is_within_suppression_window(self, days: int = 30) -> bool:
        """Check if event was published within the suppression window."""
        if not self.published_at:
            return False
        
        delta = datetime.now() - self.published_at
        return delta.days < days
    
    @classmethod
    def should_suppress_duplicate(
        cls, 
        film_slug: str, 
        event_date: date, 
        existing_events: List["PublishedEvent"],
        suppression_days: int = 30
    ) -> bool:
        """
        Determine if a new event should be suppressed due to recent publication.
        
        Args:
            film_slug: Identifier of the film
            event_date: Proposed date for new event
            existing_events: List of previously published events for this film
            suppression_days: Number of days to suppress duplicates
            
        Returns:
            True if event should be suppressed, False if it can be published
        """
        for event in existing_events:
            if event.film_slug == film_slug:
                if event.is_within_suppression_window(suppression_days):
                    return True
        
        return False


class ScrapeResult(BaseModel):
    """
    Summary of a scraping operation with metrics and status.
    
    Used for monitoring, logging, and health check endpoints to track
    scraper performance and detect issues.
    """
    
    started_at: datetime = Field(
        ...,
        description="When scraping operation started"
    )
    
    completed_at: Optional[datetime] = Field(
        default=None,
        description="When scraping operation completed"
    )
    
    success: bool = Field(
        default=False,
        description="Whether scraping completed successfully"
    )
    
    # Metrics
    pages_scraped: int = Field(
        default=0,
        ge=0,
        description="Number of listing pages processed"
    )
    
    films_discovered: int = Field(
        default=0,
        ge=0,
        description="Total number of films found"
    )
    
    foreign_films: int = Field(
        default=0,
        ge=0,
        description="Number of foreign (non-Russian) films found"
    )
    
    new_films: int = Field(
        default=0,
        ge=0,
        description="Number of films not seen in previous scrapes"
    )
    
    events_created: int = Field(
        default=0,
        ge=0,
        description="Number of new calendar events generated"
    )
    
    events_suppressed: int = Field(
        default=0,
        ge=0,
        description="Number of events suppressed due to 30-day rule"
    )
    
    # Error tracking
    errors: List[str] = Field(
        default_factory=list,
        description="List of errors encountered during scraping"
    )
    
    warnings: List[str] = Field(
        default_factory=list,
        description="List of warnings generated during scraping"
    )
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate operation duration in seconds."""
        if not self.completed_at:
            return None
        
        delta = self.completed_at - self.started_at
        return delta.total_seconds()
    
    @property
    def has_errors(self) -> bool:
        """Check if operation encountered any errors."""
        return len(self.errors) > 0
    
    def add_error(self, error: str) -> None:
        """Add an error message to the result."""
        self.errors.append(error)
        self.success = False
    
    def add_warning(self, warning: str) -> None:
        """Add a warning message to the result."""
        self.warnings.append(warning)
    
    def model_dump_metrics(self) -> dict:
        """Export metrics in Prometheus format."""
        return {
            "scrape_duration_seconds": self.duration_seconds or 0,
            "pages_scraped_total": self.pages_scraped,
            "films_discovered_total": self.films_discovered,
            "foreign_films_total": self.foreign_films,
            "new_films_total": self.new_films,
            "events_created_total": self.events_created,
            "events_suppressed_total": self.events_suppressed,
            "scrape_errors_total": len(self.errors),
            "scrape_success": 1 if self.success else 0,
        }