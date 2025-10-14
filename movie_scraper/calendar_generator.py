"""
ICS calendar generation module.

Provides CalendarGenerator that reads foreign films with upcoming sessions
from the database and emits a stable, idempotent ICS payload.
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from typing import List

from icalendar import Calendar
from icalendar import Event

from movie_scraper.database import Database
from movie_scraper.models import Film
from movie_scraper.settings import get_settings


class CalendarGenerator:
    """Generate ICS calendar content from database films.

    The generator is deterministic: same inputs yield identical output bytes.
    """

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self.settings = get_settings()

    async def generate_calendar(self) -> bytes:
        """Generate an ICS file for all foreign films with upcoming sessions."""
        # Fetch films with their next upcoming screening
        films: List[Film] = await self.db.get_foreign_films_with_upcoming_sessions()

        cal = Calendar()
        cal.add('prodid', '-//Perm Foreign Films//perm-cinema//EN')
        cal.add('version', '2.0')
        cal.add('calscale', 'GREGORIAN')
        cal.add('method', 'PUBLISH')
        cal.add('x-wr-calname', self.settings.calendar_name)
        cal.add('x-wr-caldesc', self.settings.calendar_description)

        for film in films:
            if not film.next_screening:
                continue
            event_date: date = film.next_screening.date()

            # 30-day suppression rule
            suppressed = await self.db.is_event_suppressed(film.slug, event_date, self.settings.event_retention_days)
            if suppressed:
                continue

            ev = Event()
            ev.add('uid', f"{film.slug}-{event_date.isoformat()}@perm-cinema")
            ev.add('dtstart', event_date)
            ev.add('dtend', event_date)
            ev['dtstart'].params['VALUE'] = 'DATE'
            ev['dtend'].params['VALUE'] = 'DATE'
            ev.add('dtstamp', datetime.utcnow())

            # Title and description
            payload = film.model_dump_calendar()
            ev.add('summary', payload['title'])
            ev.add('description', payload['description'])
            if payload.get('url'):
                ev.add('url', payload['url'])

            ev.add('categories', ['foreign-film', 'cinema', 'perm'])
            cal.add_component(ev)

            # Record publication to enforce the 30-day rule
            await self.db.record_published_event(film.slug, event_date)

        return cal.to_ical()
