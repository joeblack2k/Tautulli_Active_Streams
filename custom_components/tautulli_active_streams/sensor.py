import logging
import time
from datetime import datetime, timedelta
from homeassistant.config_entries import ConfigEntry
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.const import STATE_OFF, CONF_URL, CONF_API_KEY
from homeassistant.helpers.entity import EntityCategory
from homeassistant.components.sensor import SensorStateClass, SensorDeviceClass
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.event import async_track_time_interval
import aiohttp
import xml.etree.ElementTree as ET

from .const import (
    DOMAIN,
    DEFAULT_NUM_SENSORS,
    CONF_NUM_SENSORS,
    CONF_ENABLE_STATISTICS,
    CONF_IMAGE_PROXY,
    CONF_ADVANCED_ATTRIBUTES
    )

_LOGGER = logging.getLogger(__name__)


def format_seconds_to_min_sec(total_seconds: float) -> str:
    """Convert seconds into 'Mm Ss' format."""
    total_seconds = int(total_seconds)
    minutes = total_seconds // 60
    secs = total_seconds % 60
    return f"{minutes}m {secs}s"


async def _fetch_plex_metadata(plex_base_url, plex_token, rating_key):
    """
    Query Plex for metadata including chapters, markers, and other attributes.
    Returns a tuple of (credits_offset, metadata_dict, http_status).
    """
    url = (
        f"{plex_base_url}/library/metadata/{rating_key}"
        f"?includeChapters=1&includeMarkers=1"
        f"&X-Plex-Token={plex_token}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    _LOGGER.debug(
                        "Plex metadata fetch failed for rating_key=%s: status=%s, reason=%s",
                        rating_key, resp.status, resp.reason
                    )
                    return None, {}, resp.status

                # Parse XML
                xml_body = await resp.text()
                root = ET.fromstring(xml_body)
                
                # Check various XML paths for metadata
                mediacontainer = root.find("MediaContainer")
                video_el = root.find(".//Video")
                collection = root.find(".//Collection") 
                director = root.find(".//Director")
                writer = root.find(".//Writer")
                producer = root.find(".//Producer")
                role = root.find(".//Role")
                
                # If no Video element found, return empty results
                if video_el is None:
                    return None, {}, resp.status


                # Initialize metadata dict
                metadata = {}

                # 1) Credits offset from markers/chapters
                credits_offset = None
                for marker in video_el.findall("Marker"):
                    if marker.attrib.get("type") == "credits":
                        credits_offset = int(marker.attrib.get("startTimeOffset", 0))
                        break

                if not credits_offset:
                    for chapter in video_el.findall("Chapter"):
                        if "credit" in chapter.attrib.get("tag", "").lower():
                            credits_offset = int(chapter.attrib.get("startTimeOffset", 0))
                            break

                # 2) Parse Director tags
                directors = []
                for director in video_el.findall(".//Director"):
                    if "tag" in director.attrib:
                        directors.append(director.attrib["tag"])
                if directors:
                    metadata["directors"] = directors

                # 3) Parse Role/Cast tags
                cast = []
                for role in video_el.findall(".//Role"):
                    cast_entry = {
                        "actor": role.attrib.get("tag"),
                        "role": role.attrib.get("role")
                    }
                    cast.append(cast_entry)
                if cast:
                    metadata["cast"] = cast

                # 4) Parse Genre tags
                genres = []
                for genre in video_el.findall(".//Genre"):
                    if "tag" in genre.attrib:
                        genres.append(genre.attrib["tag"])
                if genres:
                    metadata["genres"] = genres

                # 5) Parse Writer tags
                writers = []
                for writer in video_el.findall(".//Writer"):
                    if "tag" in writer.attrib:
                        writers.append(writer.attrib["tag"])
                if writers:
                    metadata["writers"] = writers

                # 6) Parse Country tags
                countries = []
                for country in video_el.findall(".//Country"):
                    if "tag" in country.attrib:
                        countries.append(country.attrib["tag"])
                if countries:
                    metadata["country"] = countries[0]  # Take first country

                # 7) Parse Guid tags for external IDs
                guids = []
                for guid in video_el.findall(".//Guid"):
                    if "id" in guid.attrib:
                        guids.append(guid.attrib["id"])
                if guids:
                    metadata["guids"] = guids

                # 8) Get Media/Part info for file location
                media = video_el.find(".//Media")
                if media is not None:
                    part = media.find("Part")
                    if part is not None and "file" in part.attrib:
                        metadata["library_folder"] = part.attrib["file"]

                # 9) Get library section info from parent container
                library = root.find("LibrarySection")
                if library is not None:
                    if "title" in library.attrib:
                        metadata["library_section_title"] = library.attrib["title"]
                    if "id" in library.attrib:
                        metadata["library_section_id"] = library.attrib["id"]

                # 10) Parse Rating tags
                for rating in video_el.findall(".//Rating"):
                    image = rating.attrib.get("image", "")
                    value = rating.attrib.get("value")
                    if value:
                        if "rottentomatoes://image.rating.ripe" in image:
                            metadata["rotten_tomatoes_rating"] = value
                        elif "rottentomatoes://image.rating.upright" in image:
                            metadata["rotten_tomatoes_audience_rating"] = value
                        elif "imdb://image.rating" in image:
                            metadata["imdb_rating"] = value

                # 11) Get basic metadata from Video attributes
                basic_fields = [
                    "title", "summary", "year", "rating", "studio",
                    "tagline", "contentRating", "originallyAvailableAt",
                    "audienceRating", "viewCount", "addedAt", "updatedAt",
                    "lastViewedAt"
                ]
                for field in basic_fields:
                    if field in video_el.attrib:
                        metadata[field] = video_el.attrib[field]

                return credits_offset, metadata, 200

    except Exception as err:
        _LOGGER.debug("Error fetching Plex metadata for rating_key=%s: %s", rating_key, err)
        return None, {}, None


async def async_setup_entry(hass, entry, async_add_entities):
    """Set up the Tautulli sensors."""
    # Force a refresh to ensure we have latest data
    data = hass.data[DOMAIN][entry.entry_id]
    sessions_coordinator = data["sessions_coordinator"]
    history_coordinator = data["history_coordinator"]

    # Force a fetch so the sensors see the final plex fields
    await sessions_coordinator.async_request_refresh()
    await history_coordinator.async_request_refresh()

    # Number of active stream sensors to create
    num_sensors = entry.options.get(CONF_NUM_SENSORS, DEFAULT_NUM_SENSORS)

    # 1) Create a sensor for each "active stream" slot
    session_sensors = []
    for i in range(num_sensors):
        session_sensors.append(
            TautulliStreamSensor(sessions_coordinator, entry, i)
        )

    # 2) Create diagnostic sensors
    diagnostic_sensors = [
        TautulliDiagnosticSensor(sessions_coordinator, entry, "stream_count"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "stream_count_direct_play"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "stream_count_direct_stream"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "stream_count_transcode"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "total_bandwidth"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "lan_bandwidth"),
        TautulliDiagnosticSensor(sessions_coordinator, entry, "wan_bandwidth"),
    ]

    # 3) (Optional) Create user stats sensors if "enable_statistics" is on
    stats_sensors = []
    if entry.options.get(CONF_ENABLE_STATISTICS, False):
        user_stats = history_coordinator.data.get("user_stats", {})
        if user_stats:
            i = 0
            for username, stats_dict in user_stats.items():
                stats_sensors.append(
                    TautulliUserStatsSensor(
                        coordinator=history_coordinator,
                        entry=entry,
                        username=username,
                        stats=stats_dict,
                        index=i
                    )
                )
                i += 1
        else:
            _LOGGER.debug(
                "enable_statistics is True, but no user_stats found in history_coordinator.data."
            )

    # Add everything to Home Assistant
    async_add_entities(session_sensors, True)
    async_add_entities(diagnostic_sensors, True)
    async_add_entities(stats_sensors, True)


class TautulliStreamSensor(CoordinatorEntity, SensorEntity):
    """
    Representation of a Tautulli stream sensor,
    reading from the sessions_coordinator for session data.
    """

    def __init__(self, coordinator, entry, index):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._entry = entry
        self._index = index
        # The unique_id ends with _tautulli so the removal code can match
        self._attr_unique_id = f"plex_session_{index + 1}_{entry.entry_id}_tautulli"
        self._attr_name = f"Plex Session {index + 1} (Tautulli)"
        self._attr_icon = "mdi:plex"

        # local paused duration tracking
        self._paused_start = None
        self._paused_duration_sec = 0
        self._paused_duration_str = "0m 0s"
        self._unsub_timer = None

        # new: track credits
        self._credits_start_time = None  # e.g. "1m 23s"
        self._in_credits = False

        # Add new tracking variables
        self._last_state = STATE_OFF
        self._last_rating_key = None
        self._metadata_fetched = False
        self._auth_warning_emitted = False

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"{self._entry.entry_id}_active_streams")},
            "name": f"{self._entry.title} Active Streams",
            "manufacturer": "Richardvaio",
            "model": "Tautulli Active Streams",
            "entry_type": "service",
        }

    async def async_added_to_hass(self):
        """
        Called when this sensor is added to HA.
        We set up a per-second timer to update pause durations and credits.
        """
        await super().async_added_to_hass()
        self._unsub_timer = async_track_time_interval(
            self.hass, self._update_every_second, timedelta(seconds=1)
        )

    async def async_will_remove_from_hass(self):
        """
        Called when removing the sensor, so we cancel our timer.
        """
        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None
        await super().async_will_remove_from_hass()

    async def _update_every_second(self, now):
        """Called every second to update pause duration and credits only."""
        # 1) Update local paused-time tracking
        self._update_pause_duration()

        # 2) Check if we need to fetch metadata
        current_state = self.state
        sessions = self.coordinator.data.get("sessions", [])
        
        if len(sessions) > self._index:
            session = sessions[self._index]
            current_rating_key = session.get("rating_key")
            
            # Fetch metadata if:
            # - State changed from OFF to anything else
            # - Rating key changed (different content)
            # - Metadata hasn't been fetched yet
            if (self._last_state == STATE_OFF and current_state != STATE_OFF) or \
               (current_rating_key and current_rating_key != self._last_rating_key) or \
               (not self._metadata_fetched and current_state != STATE_OFF):
                await self._fetch_full_metadata()
            
            # Always update credits detection
            await self._update_credits_only()
            
            # Update tracking variables
            self._last_state = current_state
            self._last_rating_key = current_rating_key
        else:
            self._last_state = STATE_OFF
            self._last_rating_key = None
            self._metadata_fetched = False
            self._auth_warning_emitted = False
            self._credits_start_time = None
            self._in_credits = False

        # Finally, write new state attributes
        self.async_write_ha_state()

    async def _fetch_full_metadata(self):
        """Fetch full metadata from Plex when needed."""
        plex_enabled = self._entry.data.get("plex_enabled")
        plex_token = self._entry.data.get("plex_token")
        plex_base_url = self._entry.data.get("plex_base_url")

        if not all([plex_enabled, plex_token, plex_base_url]):
            return

        sessions = self.coordinator.data.get("sessions", [])
        if len(sessions) <= self._index:
            return

        session = sessions[self._index]
        rating_key = session.get("rating_key")
        if not rating_key:
            return

        try:
            credits_offset, metadata, status = await _fetch_plex_metadata(
                plex_base_url, plex_token, rating_key
            )

            if status in (401, 403):
                if not self._auth_warning_emitted:
                    _LOGGER.warning(
                        "Plex metadata authorization failed (status=%s). "
                        "Skipping metadata enrichment for this stream; check plex_token in the "
                        "'%s' config entry.",
                        status,
                        self._entry.title,
                    )
                    self._auth_warning_emitted = True
                # Prevent per-second retry storms for the same session.
                self._metadata_fetched = True
                self._credits_start_time = None
                return

            if status and status != 200:
                # Prevent per-second retry storms for the same session.
                self._metadata_fetched = True
                self._credits_start_time = None
                return
            
            # Store credits offset for future checks
            if credits_offset:
                minutes = credits_offset // 60000
                seconds = (credits_offset % 60000) // 1000
                self._credits_start_time = f"{minutes}m {seconds}s"
            else:
                self._credits_start_time = None

            # Update session metadata in coordinator
            if metadata and "sessions" in self.coordinator.data:
                self.coordinator.data["sessions"][self._index].update(metadata)
            # Mark this stream as attempted, even when metadata is empty.
            self._metadata_fetched = True
            self._auth_warning_emitted = False

        except Exception as err:
            _LOGGER.warning("Error fetching full metadata: %s", err)

    async def _update_credits_only(self):
        """Only check credits position, no full metadata fetch."""
        if not self._credits_start_time:
            return

        session = self.coordinator.data["sessions"][self._index]
        view_offset_str = session.get("view_offset")
        
        try:
            view_offset = int(view_offset_str)
            credits_ms = sum(
                x * y for x, y in zip(
                    map(int, self._credits_start_time.replace('m','').replace('s','').split()),
                    (60000, 1000)
                )
            )
            self._in_credits = (view_offset >= credits_ms)
        except (ValueError, TypeError):
            self._in_credits = False

    def _update_pause_duration(self):
        """
        Increments local pause counter if the state is 'paused'.
        Resets if it's not paused.
        """
        current_state = self.state.lower()
        if current_state == "paused":
            if self._paused_start is None:
                self._paused_start = time.time()
            elapsed = time.time() - self._paused_start
            self._paused_duration_sec = int(elapsed)
            self._paused_duration_str = format_seconds_to_min_sec(self._paused_duration_sec)
        else:
            self._paused_start = None
            self._paused_duration_sec = 0
            self._paused_duration_str = "0m 0s"

    @property
    def state(self):
        """Return the current Tautulli session state (playing, paused, etc.)"""
        sessions = self.coordinator.data.get("sessions", [])
        if len(sessions) > self._index:
            return sessions[self._index].get("state", STATE_OFF)
        return STATE_OFF

    @property
    def extra_state_attributes(self):
        """
        Return extra attributes for the sensor (basic or advanced),
        plus new 'in_credits' info if Plex integration is enabled.
        """
        plex_enabled = self._entry.data.get("plex_enabled")
        plex_token = self._entry.data.get("plex_token")
        plex_base_url = self._entry.data.get("plex_base_url")

        sessions = self.coordinator.data.get("sessions", [])
        if len(sessions) <= self._index:
            return {}

        session = sessions[self._index]

        base_url = self._entry.data.get(CONF_URL)
        api_key = self._entry.data.get(CONF_API_KEY)
        image_proxy = self._entry.options.get(CONF_IMAGE_PROXY, False)
        advanced = self._entry.options.get(CONF_ADVANCED_ATTRIBUTES, False)

        attributes = {}

        # Build an image URL if base_url & api_key
        thumb_url = session.get("grandparent_thumb") or session.get("thumb")
        if thumb_url and base_url and api_key:
            if image_proxy:
                attributes["image_url"] = (
                    f"/api/tautulli/image"
                    f"?entry_id={self._entry.entry_id}"
                    f"&img={thumb_url}"
                    "&width=300&height=450&fallback=poster&refresh=true"
                )
            else:
                attributes["image_url"] = (
                    f"{base_url}/api/v2"
                    f"?apikey={api_key}"
                    f"&cmd=pms_image_proxy"
                    f"&img={thumb_url}"
                    "&width=300&height=450&fallback=poster&refresh=true"
                )

        # Build an art URL if base_url & api_key
        art_path = session.get("art")
        if art_path and base_url and api_key:
            if image_proxy:
                attributes["art_url"] = (
                    f"/api/tautulli/image"
                    f"?entry_id={self._entry.entry_id}"
                    f"&img={art_path}"
                    "&width=1920&height=1080&fallback=art&refresh=true"
                )
            else:
                attributes["art_url"] = (
                    f"{base_url}/api/v2"
                    f"?apikey={api_key}"
                    f"&cmd=pms_image_proxy"
                    f"&img={art_path}"
                    "&width=1920&height=1080&fallback=art&refresh=true"
                )

        # Basic
        attributes["user"] = session.get("user")
        attributes["progress_percent"] = session.get("progress_percent")
        attributes["media_type"] = session.get("media_type")
        attributes["full_title"] = session.get("full_title")
        attributes["parent_media_index"] = session.get("parent_media_index")
        attributes["media_index"] = session.get("media_index")
        attributes["year"] = session.get("year")
        attributes["product"] = session.get("product")
        attributes["player"] = session.get("player")
        attributes["device"] = session.get("device")
        attributes["platform"] = session.get("platform")
        attributes["location"] = session.get("location")
        attributes["ip_address"] = session.get("ip_address")
        attributes["ip_address_public"] = session.get("ip_address_public")
        attributes["geo_city"] = session.get("geo_city")
        attributes["geo_region"] = session.get("geo_region")
        attributes["geo_country"] = session.get("geo_country")
        attributes["geo_code"] = session.get("geo_code")
        attributes["local"] = session.get("local")
        attributes["relayed"] = session.get("relayed")
        attributes["bandwidth"] = session.get("bandwidth")
        attributes["video_resolution"] = session.get("video_resolution")
        attributes["stream_video_resolution"] = session.get("stream_video_resolution")
        attributes["transcode_decision"] = session.get("transcode_decision")
        attributes["stream_paused_duration"] = self._paused_duration_str

        # If advanced is off, return now
        if advanced:

            # Advanced is ON, so add more
            if session.get("stream_duration"):
                total_ms = float(session["stream_duration"])
                hours = int(total_ms // 3600000)
                minutes = int((total_ms % 3600000) // 60000)
                seconds = int((total_ms % 60000) // 1000)
                formatted_duration = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                formatted_duration = None

            if session.get("view_offset") and session.get("stream_duration"):
                remain_ms = float(session["stream_duration"]) - float(session["view_offset"])
                remain_seconds = remain_ms / 1000
                remain_hours = int(remain_seconds // 3600)
                remain_minutes = int((remain_seconds % 3600) // 60)
                remain_secs = int(remain_seconds % 60)
                formatted_remaining = f"{remain_hours}:{remain_minutes:02d}:{remain_secs:02d}"

                eta = datetime.now() + timedelta(seconds=remain_seconds)
                hour_12 = eta.strftime("%I").lstrip("0") or "12"
                minute = eta.strftime("%M")
                ampm = eta.strftime("%p").lower()
                formatted_eta = f"{hour_12}:{minute} {ampm}"
            else:
                formatted_remaining = None
                formatted_eta = None

            attributes.update({
                "user_friendly_name": session.get("friendly_name"),
                "username": session.get("username"),
                "user_thumb": session.get("user_thumb"),
                "session_id": session.get("session_id"),
                "library_name": session.get("library_name"),
                "grandparent_title": session.get("grandparent_title"),
                "title": session.get("title"),
                "container": session.get("container"),
                "aspect_ratio": session.get("aspect_ratio"),
                "video_codec": session.get("video_codec"),
                "video_framerate": session.get("video_framerate"),
                "video_profile": session.get("video_profile"),
                "video_dovi_profile": session.get("video_dovi_profile"),
                "video_dynamic_range": session.get("video_dynamic_range"),
                "video_color_space": session.get("video_color_space"),
                "audio_codec": session.get("audio_codec"),
                "audio_channels": session.get("audio_channels"),
                "audio_channel_layout": session.get("audio_channel_layout"),
                "audio_profile": session.get("audio_profile"),
                "audio_bitrate": session.get("audio_bitrate"),
                "audio_language": session.get("audio_language"),
                "audio_language_code": session.get("audio_language_code"),
                "subtitle_language": session.get("subtitle_language"),
                "container_decision": session.get("stream_container_decision"),
                "audio_decision": session.get("audio_decision"),
                "video_decision": session.get("video_decision"),
                "subtitle_decision": session.get("subtitle_decision"),
                "transcode_container": session.get("transcode_container"),
                "transcode_audio_codec": session.get("transcode_audio_codec"),
                "transcode_video_codec": session.get("transcode_video_codec"),
                "transcode_throttled": session.get("transcode_throttled"),
                "transcode_progress": session.get("transcode_progress"),
                "transcode_speed": session.get("transcode_speed"),
                "stream_start_time": session.get("start_time"),
                "stream_duration": formatted_duration,
                "stream_remaining": formatted_remaining,
                "stream_eta": formatted_eta,
                "stream_video_resolution": session.get("stream_video_resolution"),
                "stream_container": session.get("stream_container"),
                "stream_bitrate": session.get("stream_bitrate"),
                "stream_video_bitrate": session.get("stream_video_bitrate"),
                "stream_video_codec": session.get("stream_video_codec"),
                "stream_video_framerate": session.get("stream_video_framerate"),
                "stream_video_full_resolution": session.get("stream_video_full_resolution"),
                "stream_video_dovi_profile": session.get("stream_video_dovi_profile"),
                "stream_video_decision": session.get("stream_video_decision"),
                "stream_audio_bitrate": session.get("stream_audio_bitrate"),
                "stream_audio_codec": session.get("stream_audio_codec"),
                "stream_audio_channels": session.get("stream_audio_channels"),
                "stream_audio_channel_layout": session.get("stream_audio_channel_layout"),
                "stream_audio_language": session.get("stream_audio_language"),
                "stream_audio_language_code": session.get("stream_audio_language_code"),
            })

        # ------------------------------------------------------
        # PLEX ATTRIBUTES (if plex_enabled == True)
        # ------------------------------------------------------
        if plex_enabled and plex_token and plex_base_url:
            # Directors
            directors = []
            if isinstance(session.get("directors"), list):
                directors.extend(session["directors"])
            elif isinstance(session.get("Director"), list):
                directors.extend([d.get("tag") for d in session["Director"]])
            elif isinstance(session.get("Director"), dict):
                directors.append(session["Director"].get("tag"))
            if directors:
                attributes["directors"] = directors

            # Cast/Roles
            cast = []
            if isinstance(session.get("cast"), list):
                cast.extend(session["cast"])
            elif isinstance(session.get("Role"), list):
                cast.extend([{"actor": r.get("tag"), "role": r.get("role")} for r in session["Role"]])
            elif isinstance(session.get("Role"), dict):
                cast.append({"actor": session["Role"].get("tag"), "role": session["Role"].get("role")})
            if cast:
                attributes["cast"] = cast

            # Writers
            writers = []
            if isinstance(session.get("writers"), list):
                writers.extend(session["writers"])
            elif isinstance(session.get("Writer"), list):
                writers.extend([w.get("tag") for w in session["Writer"]])
            elif isinstance(session.get("Writer"), dict):
                writers.append(session["Writer"].get("tag"))
            if writers:
                attributes["writers"] = writers

            # Genres
            genres = []
            if isinstance(session.get("genres"), list):
                genres.extend(session["genres"])
            elif isinstance(session.get("Genre"), list):
                genres.extend([g.get("tag") for g in session["Genre"]])
            elif isinstance(session.get("Genre"), dict):
                genres.append(session["Genre"].get("tag"))
            if genres:
                attributes["genres"] = genres

            # Country
            country = session.get("country") or (
                session.get("Country", {}).get("tag") if isinstance(session.get("Country"), dict) else None
            )
            if country:
                attributes["country"] = country

            # External IDs (GUIDs)
            guids = []
            if isinstance(session.get("guids"), list):
                guids.extend(session["guids"])
            elif isinstance(session.get("Guid"), list):
                guids.extend([g.get("id") for g in session["Guid"] if g.get("id")])
            elif isinstance(session.get("Guid"), dict):
                guid_id = session["Guid"].get("id")
                if guid_id:
                    guids.append(guid_id)
            if guids:
                attributes["guids"] = guids

            # Library Info
            library_folder = session.get("library_folder") or session.get("Part", {}).get("file")
            if library_folder:
                attributes["library_folder"] = library_folder

            library_section_title = session.get("library_section_title") or session.get("librarySectionTitle")
            if library_section_title:
                attributes["library_section_title"] = library_section_title

            library_section_id = session.get("library_section_id") or session.get("librarySectionID")
            if library_section_id:
                attributes["library_section_id"] = library_section_id

            # Ratings
            ratings_mapping = {
                "rottentomatoes://image.rating.ripe": "rotten_tomatoes_rating",
                "rottentomatoes://image.rating.upright": "rotten_tomatoes_audience_rating",
                "imdb://image.rating": "imdb_rating"
            }

            # Check both flattened and nested rating structures
            for rating_type, attr_name in ratings_mapping.items():
                rating_value = None
                # Check flattened structure
                if session.get(attr_name):
                    rating_value = session[attr_name]
                # Check nested Rating structure
                elif isinstance(session.get("Rating"), list):
                    for rating in session["Rating"]:
                        if rating.get("image") == rating_type:
                            rating_value = rating.get("value")
                            break
                if rating_value:
                    attributes[attr_name] = rating_value

            # Basic Metadata
            basic_fields = {
                "tagline": ["tagline"],
                "summary": ["summary"],
                "studio": ["studio"],
                "content_rating": ["content_rating", "contentRating"],
                "rating": ["rating"],
                "audience_rating": ["audience_rating", "audienceRating"],
                "originally_available_at": ["originally_available_at", "originallyAvailableAt"],
                "rating_key": ["rating_key"],
            }

            for attr_name, possible_keys in basic_fields.items():
                for key in possible_keys:
                    value = session.get(key)
                    if value:
                        attributes[attr_name] = value
                        break

            # Timestamps
            timestamp_fields = ["addedAt", "updatedAt", "lastViewedAt"]
            for field in timestamp_fields:
                try:
                    timestamp = session.get(field)
                    if timestamp is not None and timestamp != "":
                        # Convert string to float if needed
                        if isinstance(timestamp, str):
                            timestamp = float(timestamp)
                        elif not isinstance(timestamp, (int, float)):
                            continue
                        # Create datetime object and format
                        try:
                            date = datetime.fromtimestamp(timestamp)
                            field_name = field[0].lower() + field[1:]  # camelCase to snake_case
                            attributes[field_name] = date.strftime("%Y-%m-%d %H:%M:%S")
                        except (ValueError, OSError) as err:
                            _LOGGER.debug("Invalid timestamp value for %s=%s: %s", field, timestamp, err)
                            
                except (ValueError, TypeError) as err:
                    _LOGGER.debug("Could not process timestamp field %s: %s", field, err)

            # View Count
            view_count = session.get("view_count") or session.get("viewCount")
            if view_count is not None:
                try:
                    attributes["view_count"] = int(view_count)
                except (ValueError, TypeError):
                    _LOGGER.debug("Invalid view count value: %s", view_count)

            # Credits Information
            attributes["in_credits"] = self._in_credits
            if self._credits_start_time:
                attributes["credits_start_time"] = self._credits_start_time

        return attributes



class TautulliDiagnosticSensor(CoordinatorEntity, SensorEntity):
    """
    Representation of a Tautulli diagnostic sensor,
    also using the sessions_coordinator to read 'diagnostics'.
    """

    def __init__(self, coordinator, entry, metric):
        """Initialize the diagnostic sensor."""
        super().__init__(coordinator)
        self._entry = entry
        self._metric = metric
        self._attr_unique_id = f"tautulli_{entry.entry_id}_{metric}"
        self.entity_id = f"sensor.tautulli_{metric}"
        self._attr_name = f"{metric.replace('_', ' ').title()}"
        self._attr_entity_category = EntityCategory.DIAGNOSTIC
        self._attr_device_info = self.device_info
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_device_class = SensorDeviceClass.DATA_SIZE

        if metric in ["total_bandwidth", "lan_bandwidth", "wan_bandwidth"]:
            self._attr_native_unit_of_measurement = "Mbps"
        else:
            self._attr_native_unit_of_measurement = None

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"{self._entry.entry_id}_active_streams")},
            "name": f"{self._entry.title} Active Streams",
            "manufacturer": "Richardvaio",
            "model": "Tautulli Active Streams",
            "entry_type": "service",
        }

    @property
    def state(self):
        """Return the main diagnostic value from 'diagnostics'."""
        diagnostics = self.coordinator.data.get("diagnostics", {})
        raw_value = diagnostics.get(self._metric, 0)

        if self._metric in ["total_bandwidth", "lan_bandwidth", "wan_bandwidth"]:
            try:
                return round(float(raw_value) / 1000, 1)
            except Exception as err:
                _LOGGER.error("Error converting bandwidth: %s", err)
                return raw_value

        return raw_value

    @property
    def extra_state_attributes(self):
        """Return additional diagnostic attributes (e.g. session list)."""
        if self._metric != "stream_count":
            return {}
        sessions = self.coordinator.data.get("sessions", [])
        filtered_sessions = []
        for s in sessions:
            filtered_sessions.append({
                "username": (s.get("username") or "").lower(),
                "user": (s.get("user") or "").lower(),
                "full_title": s.get("full_title"),
                "stream_start_time": s.get("start_time"),
                "start_time_raw": s.get("start_time_raw"),
                "Stream_paused_duration_sec": s.get("Stream_paused_duration_sec"),
                "session_id": s.get("session_id"),
            })
        return {"sessions": filtered_sessions}

    @property
    def icon(self):
        icon_map = {
            "stream_count": "mdi:plex",
            "stream_count_direct_play": "mdi:play-circle",
            "stream_count_direct_stream": "mdi:play-network",
            "stream_count_transcode": "mdi:cog",
            "total_bandwidth": "mdi:download-network",
            "lan_bandwidth": "mdi:lan",
            "wan_bandwidth": "mdi:wan",
        }
        return icon_map.get(self._metric, "mdi:chart-bar")


class TautulliUserStatsSensor(CoordinatorEntity, SensorEntity):
    """
    One sensor per user, each with '_stats_' in its unique_id,
    referencing history_coordinator.data for user_stats.
    """

    def __init__(self, coordinator, entry: ConfigEntry, username: str, stats: dict, index: int):
        super().__init__(coordinator)
        self._entry = entry
        self._username = username
        self._stats = stats
        self._index = index

        # Must have "_stats_" so the removal code can detect it
        self._attr_unique_id = f"{entry.entry_id}_{username.lower()}_{index}_stats_"
        self._attr_name = f"{username} Stats"

        # Put these sensors under a separate device named "<Integration Title> Statistics"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, f"{entry.entry_id}_statistics_device")},
            "name": f"{entry.title} Statistics",
            "manufacturer": "Richardvaio",
            "model": "Tautulli Statistics",
        }

    @property
    def icon(self) -> str:
        return "mdi:account"
        
    @property
    def state(self):
        user_stats = self.coordinator.data["user_stats"].get(self._username, {})
        return user_stats.get("total_play_duration", "0h 0m")

    @property
    def extra_state_attributes(self):
        """Return watch-history stats from self._stats (parsed from get_history)."""
        return {
            # --- Basic Play Counts ---
            "total_plays": self._stats.get("total_plays", 0),
            "movie_plays": self._stats.get("movie_plays", 0),
            "tv_plays": self._stats.get("tv_plays", 0),

            # --- Duration & Completion & Pause Metrics ---
            "total_play_duration": self._stats.get("total_play_duration", "0h 0m"),
            "total_completion_rate": self._stats.get("total_completion_rate", 0.0),
            "longest_play": self._stats.get("longest_play", "0h 0m"),
            "average_play_gap": self._stats.get("average_play_gap", "N/A"),
            "paused_count": self._stats.get("paused_count", 0),
            "total_paused_duration": self._stats.get("total_paused_duration", "0h 0m"),

            # --- Popular Titles ---
            "most_popular_show": self._stats.get("most_popular_show", ""),
            "most_popular_movie": self._stats.get("most_popular_movie", ""),
            
            # --- Watch Times --- Weekday & Gaps ---
            "days_since_last_watch": self._stats.get("days_since_last_watch"),
            "preferred_watch_time": self._stats.get("preferred_watch_time", ""),
            "weekday_plays": self._stats.get("weekday_plays", []),
            "watched_morning": self._stats.get("watched_morning", 0),
            "watched_afternoon": self._stats.get("watched_afternoon", 0),
            "watched_midday": self._stats.get("watched_midday", 0),
            "watched_evening": self._stats.get("watched_evening", 0),
            
            # --- Transcode / Playback Types ---
            "transcode_count": self._stats.get("transcode_count", 0),
            "direct_play_count": self._stats.get("direct_play_count", 0),
            "direct_stream_count": self._stats.get("direct_stream_count", 0),
            "transcode_percentage": self._stats.get("transcode_percentage", 0.0),
            "common_transcode_devices": self._stats.get("common_transcode_devices", ""),
            "last_transcode_date": self._stats.get("last_transcode_date", ""),

            # --- Device Usage ---
            "most_used_device": self._stats.get("most_used_device", ""),
            "common_audio_language": self._stats.get("common_audio_language", "Unknown"),
            
            # --- Geo Location ---
            "geo_city": self._stats.get("geo_city"),
            "geo_region": self._stats.get("geo_region"),
            "geo_country": self._stats.get("geo_country"),
            "geo_code": self._stats.get("geo_code"),
            "geo_latitude": self._stats.get("geo_latitude"),
            "geo_longitude": self._stats.get("geo_longitude"),
            "geo_continent": self._stats.get("geo_continent"),
            "geo_postal_code": self._stats.get("geo_postal_code"),
            "geo_timezone": self._stats.get("geo_timezone"),

            # --- LAN vs WAN ---
            "lan_plays": self._stats.get("lan_plays", 0),
            "wan_plays": self._stats.get("wan_plays", 0),
        }

    async def async_update(self):
        """
        If the coordinator data changes, re-fetch this user's stats
        from history_coordinator.data["user_stats"] if needed.
        """
        all_stats = self.coordinator.data.get("user_stats", {})
        self._stats = all_stats.get(self._username, {})


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """
    This is called when the user changes options in the UI.
    If stats are disabled, remove existing stats sensors/device.
    Then reload the config entry so the updated sensor count or stats toggle
    can be applied to the sensor platform.
    """
    enable_stats = entry.options.get(CONF_ENABLE_STATISTICS, False)
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    # If user turned stats off, remove all stats sensors & device
    if not enable_stats:
        for entity_entry in list(ent_reg.entities.values()):
            if (
                entity_entry.config_entry_id == entry.entry_id
                and "_stats_" in (entity_entry.unique_id or "")
            ):
                ent_reg.async_remove(entity_entry.entity_id)

        # remove the stats device
        for device_entry in list(dev_reg.devices.values()):
            if (
                entry.entry_id in device_entry.config_entries
                and any(
                    iden[0] == DOMAIN and iden[1] == f"{entry.entry_id}_statistics_device"
                    for iden in device_entry.identifiers
                )
            ):
                dev_reg.async_remove_device(device_entry.id)

    # Force a fresh Tautulli fetch (if your coordinator is sessions_coordinator):
    sessions_coordinator = hass.data[DOMAIN][entry.entry_id]["sessions_coordinator"]
    await sessions_coordinator.async_request_refresh()

    # Reload the config entry so the changes take effect
    await hass.config_entries.async_reload(entry.entry_id)
