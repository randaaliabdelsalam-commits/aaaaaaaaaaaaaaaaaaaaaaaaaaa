"""Alert helpers for operational failures."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess

log = logging.getLogger(__name__)


def play_alert_sound() -> bool:
    """Play the configured alert sound if possible.

    Returns True when a player command was successfully invoked.
    """
    sound_path = os.getenv("ALERT_SOUND_PATH")
    if not sound_path:
        log.error("alert_triggered", extra={"reason": "missing_alert_sound_path"})
        return False

    players = ("afplay", "paplay", "aplay", "ffplay")
    player = next((cmd for cmd in players if shutil.which(cmd)), None)
    if not player:
        log.error(
            "alert_triggered",
            extra={"reason": "no_audio_player", "sound_path": sound_path},
        )
        return False

    cmd = [player, sound_path]
    if player == "ffplay":
        cmd = [player, "-nodisp", "-autoexit", "-loglevel", "error", sound_path]

    try:
        subprocess.run(cmd, check=False, capture_output=True)
    except OSError:
        log.exception(
            "alert_triggered",
            extra={"reason": "player_failed", "player": player, "sound_path": sound_path},
        )
        return False

    return True
