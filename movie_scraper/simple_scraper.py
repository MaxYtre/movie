"""
Упрощенный скрапер для кинотеатров Перми.

Создает ICS календарь иностранных фильмов для GitHub Pages.
"""

import asyncio
import json
import logging
import re
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from icalendar import Calendar
from icalendar import Event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilmInfo:
    """Информация о фильме."""
    
    def __init__(self, title: str, country: str, url: str, next_date: Optional[date] = None):
        self.title = title
        self.country = country
        self.url = url
        self.next_date = next_date
        self.description = ""
        self.rating = ""
        self.age_limit = ""
        
    @property
    def is_foreign(self) -> bool:
        """Проверяет, является ли фильм иностранным."""
        russian_keywords = ['россия', 'russia', 'рф', 'ссср', 'ussr']
        return not any(keyword in self.country.lower() for keyword in russian_keywords)
    
    def __repr__(self) -> str:
        return f"FilmInfo({self.title}, {self.country}, foreign={self.is_foreign})"


class PermCinemaScraper:
    """Скрапер для афиша.ru Пермь."""
    
    def __init__(self):
        self.base_url = "https://www.afisha.ru"
        self.perm_url = f"{self.base_url}/prm/schedule_cinema/"
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        self.session = aiohttp.ClientSession(
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            connector=connector,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _fetch_with_retry(self, url: str, retries: int = 3) -> Optional[str]:
        """Получает HTML с повторными попытками."""
        for attempt in range(retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt + 1})")
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        # Rate limited - wait longer
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        
            except Exception as e:
                logger.warning(f"Request failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                
        return None
    
    async def scrape_film_listings(self) -> List[str]:
        """Получает список URL фильмов."""
        film_urls = []
        page = 1
        
        while page <= 5:  # Ограничиваем для безопасности
            if page == 1:
                url = self.perm_url
            else:
                url = f"{self.perm_url}page{page}/"
            
            html = await self._fetch_with_retry(url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            page_urls = self._extract_film_urls(soup)
            
            if not page_urls:
                logger.info(f"No films found on page {page}, stopping")
                break
                
            film_urls.extend(page_urls)
            logger.info(f"Found {len(page_urls)} films on page {page}")
            page += 1
            
            # Пауза между запросами
            await asyncio.sleep(1)
        
        return list(set(film_urls))  # Убираем дубликаты
    
    def _extract_film_urls(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает URL фильмов со страницы расписания."""
        urls = []
        
        # Попробуем разные селекторы для поиска ссылок на фильмы
        selectors = [
            'a[href*="/prm/schedule_cinema_product/"]',
            'a[href*="/schedule_cinema_product/"]',
            '.b-object-item h3 a',
            '.film-title a',
            'h3 a[href*="cinema"]',
            '.object-summary-title-link'
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href and '/schedule_cinema_product/' in href:
                    full_url = urljoin(self.base_url, href)
                    urls.append(full_url)
        
        return urls
    
    async def get_film_info(self, film_url: str) -> Optional[FilmInfo]:
        """Получает информацию о фильме."""
        html = await self._fetch_with_retry(film_url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Извлекаем название
        title = self._extract_title(soup)
        if not title:
            return None
            
        # Извлекаем страну
        country = self._extract_country(soup)
        if not country:
            return None
            
        # Создаем объект фильма
        film = FilmInfo(title, country, film_url)
        
        # Проверяем, является ли фильм иностранным
        if not film.is_foreign:
            logger.debug(f"Skipping Russian film: {title} ({country})")
            return None
            
        # Ищем ближайший сеанс
        film.next_date = self._find_next_screening(soup)
        
        # Дополнительная информация
        film.description = self._extract_description(soup)
        film.rating = self._extract_rating(soup)
        film.age_limit = self._extract_age_limit(soup)
        
        return film
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлекает название фильма."""
        selectors = ['h1', '.object-summary-title', '.film-title', '.b-object-title']
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                return elem.get_text(strip=True)
        
        return None
    
    def _extract_country(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлекает страну производства."""
        # Ищем в информации о фильме
        text_content = soup.get_text()
        
        # Паттерны для поиска страны
        patterns = [
            r'Страна[:\s]+([^\n,]+)',
            r'страна[:\s]+([^\n,]+)',
            r'Country[:\s]+([^\n,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                country = match.group(1).strip()
                # Очищаем от лишних символов
                country = re.sub(r'[\r\n\t]+', ' ', country)
                return country
        
        # Попробуем найти в мета-тегах или структурированных данных
        meta_selectors = [
            '[itemprop="countryOfOrigin"]',
            '.country',
            '.film-country',
            '.object-country'
        ]
        
        for selector in meta_selectors:
            elem = soup.select_one(selector)
            if elem:
                return elem.get_text(strip=True)
        
        # Фоллбэк: если не нашли страну, считаем российским
        logger.warning(f"Could not determine country, assuming Russian")
        return "Россия"
    
    def _find_next_screening(self, soup: BeautifulSoup) -> Optional[date]:
        """Находит дату ближайшего сеанса."""
        today = date.today()
        
        # Ищем даты в тексте
        text = soup.get_text()
        date_patterns = [
            r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)',
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})'
        ]
        
        found_dates = []
        
        for pattern in date_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    if len(match) == 2 and isinstance(match[1], str):  # месяц словом
                        day, month_name = match
                        month_map = {
                            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
                        }
                        if month_name in month_map:
                            screening_date = date(today.year, month_map[month_name], int(day))
                            if screening_date >= today:
                                found_dates.append(screening_date)
                except ValueError:
                    continue
        
        if found_dates:
            return min(found_dates)  # Ближайшая дата
        
        # Если не нашли дату, используем завтра
        return today + timedelta(days=1)
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Извлекает описание фильма."""
        selectors = ['.annotation', '.description', '.film-description', '.b-object-lead']
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                desc = elem.get_text(strip=True)
                return desc[:300] + "..." if len(desc) > 300 else desc
        
        return ""
    
    def _extract_rating(self, soup: BeautifulSoup) -> str:
        """Извлекает рейтинг фильма."""
        text = soup.get_text()
        
        # Ищем рейтинги
        rating_patterns = [
            r'IMDb[:\s]+(\d+\.\d+)',
            r'Кинопоиск[:\s]+(\d+\.\d+)',
            r'рейтинг[:\s]+(\d+\.\d+)',
        ]
        
        for pattern in rating_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return ""
    
    def _extract_age_limit(self, soup: BeautifulSoup) -> str:
        """Извлекает возрастное ограничение."""
        text = soup.get_text()
        
        age_patterns = [
            r'(\d+\+)',
            r'возраст[:\s]+(\d+\+)',
        ]
        
        for pattern in age_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return ""


class ICSCalendarGenerator:
    """Генератор ICS календаря."""
    
    @staticmethod
    def generate_calendar(films: List[FilmInfo]) -> bytes:
        """Создает ICS календарь из списка фильмов."""
        cal = Calendar()
        cal.add('prodid', '-//Perm Foreign Films//perm-cinema//EN')
        cal.add('version', '2.0')
        cal.add('calscale', 'GREGORIAN')
        cal.add('method', 'PUBLISH')
        cal.add('x-wr-calname', 'Зарубежные фильмы в кинотеатрах Перми')
        cal.add('x-wr-caldesc', 'Иностранные фильмы, идущие в кинотеатрах Перми. Обновляется ежедневно.')
        
        for film in films:
            if not film.next_date:
                continue
                
            event = Event()
            
            # Уникальный идентификатор
            slug = film.url.split('/')[-2] if film.url.endswith('/') else film.url.split('/')[-1]
            event.add('uid', f"{slug}-{film.next_date.isoformat()}@perm-cinema")
            
            # Даты (весь день)
            event.add('dtstart', film.next_date)
            event.add('dtend', film.next_date)
            event.add('dtstamp', datetime.now())
            
            # Указываем, что это событие на весь день
            event['dtstart'].params['VALUE'] = 'DATE'
            event['dtend'].params['VALUE'] = 'DATE'
            
            # Название события
            title = film.title
            if film.age_limit:
                title += f" ({film.age_limit})"
            event.add('summary', title)
            
            # Описание
            description_parts = []
            if film.country:
                description_parts.append(f"Страна: {film.country}")
            if film.rating:
                description_parts.append(f"Рейтинг: {film.rating}")
            if film.description:
                description_parts.append(f"\nОписание: {film.description}")
            if film.url:
                description_parts.append(f"\nПодробнее: {film.url}")
            
            event.add('description', '\n'.join(description_parts))
            
            # URL источника
            if film.url:
                event.add('url', film.url)
            
            # Категории для фильтрации
            event.add('categories', ['ЗАРУБЕЖНЫЕ-ФИЛЬМЫ', 'КИНО', 'ПЕРМЬ'])
            
            cal.add_component(event)
        
        return cal.to_ical()


async def main():
    """Главная функция для запуска скрапера."""
    logger.info("Starting Perm cinema scraper...")
    
    try:
        async with PermCinemaScraper() as scraper:
            # Получаем список фильмов
            logger.info("Fetching film listings...")
            film_urls = await scraper.scrape_film_listings()
            logger.info(f"Found {len(film_urls)} films to process")
            
            # Обрабатываем фильмы
            foreign_films = []
            
            for i, url in enumerate(film_urls[:20], 1):  # Ограничиваем для тестирования
                logger.info(f"Processing film {i}/{min(20, len(film_urls))}: {url}")
                
                film_info = await scraper.get_film_info(url)
                if film_info and film_info.is_foreign:
                    foreign_films.append(film_info)
                    logger.info(f"Added foreign film: {film_info.title} ({film_info.country})")
                
                # Пауза между запросами
                await asyncio.sleep(1)
            
            logger.info(f"Found {len(foreign_films)} foreign films")
            
            # Создаем календарь
            if foreign_films:
                ics_data = ICSCalendarGenerator.generate_calendar(foreign_films)
                
                # Сохраняем файл
                docs_dir = Path("docs")
                docs_dir.mkdir(exist_ok=True)
                
                calendar_path = docs_dir / "calendar.ics"
                with open(calendar_path, 'wb') as f:
                    f.write(ics_data)
                
                logger.info(f"Calendar saved to {calendar_path} with {len(foreign_films)} films")
                
                # Создаем индексную страницу
                create_index_page(docs_dir, len(foreign_films))
                
            else:
                logger.warning("No foreign films found, not generating calendar")
                
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        raise


def create_index_page(docs_dir: Path, film_count: int):
    """Создает индексную страницу для GitHub Pages."""
    html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Календарь зарубежных фильмов в Перми</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .calendar-link {{
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 4px;
            margin: 10px 0;
        }}
        .calendar-link:hover {{
            background: #45a049;
        }}
        .instructions {{
            background: #e8f5e8;
            padding: 20px;
            border-radius: 4px;
            margin: 20px 0;
        }}
        .stats {{
            color: #666;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎬 Календарь зарубежных фильмов в Перми</h1>
        <p>Автоматически обновляемый календарь иностранных фильмов, идущих в кинотеатрах Перми.</p>
        
        <a href="calendar.ics" class="calendar-link" download>📅 Скачать календарь (.ics)</a>
        
        <div class="instructions">
            <h3>Как подключить к Google Calendar:</h3>
            <ol>
                <li>Скопируйте эту ссылку: <code>https://maxytree.github.io/movie/calendar.ics</code></li>
                <li>Откройте Google Calendar</li>
                <li>Слева нажмите "+" рядом с "Другие календари"</li>
                <li>Выберите "Из URL"</li>
                <li>Вставьте скопированную ссылку</li>
                <li>Нажмите "Добавить календарь"</li>
            </ol>
        </div>
        
        <div class="instructions">
            <h3>Для других календарных приложений:</h3>
            <ul>
                <li><strong>Apple Calendar:</strong> Файл → Подписка на календарь</li>
                <li><strong>Outlook:</strong> Добавить календарь → Подписаться из Интернета</li>
                <li><strong>Другие:</strong> Скачайте файл .ics и импортируйте его</li>
            </ul>
        </div>
        
        <div class="stats">
            <p>📊 Сейчас в календаре: {film_count} зарубежных фильмов</p>
            <p>🔄 Обновляется ежедневно в 11:00 по времени Перми</p>
            <p>📅 События создаются на весь день (без конкретного времени сеанса)</p>
            <p>⏰ Последнее обновление: {datetime.now().strftime('%d.%m.%Y в %H:%M')}</p>
        </div>
        
        <hr>
        <p><small>
            Источник данных: <a href="https://www.afisha.ru/prm/schedule_cinema/" target="_blank">afisha.ru</a> | 
            Код проекта: <a href="https://github.com/MaxYtre/movie" target="_blank">GitHub</a>
        </small></p>
    </div>
</body>
</html>
"""
    
    index_path = docs_dir / "index.html"
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"Index page created at {index_path}")


if __name__ == "__main__":
    asyncio.run(main())