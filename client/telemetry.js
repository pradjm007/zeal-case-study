class TelemetryCollector {
  constructor(hls, video) {
    this.hls = hls;
    this.video = video;
    this.sessionId = this.generateId();
    this.viewerId = "viewer_" + Math.floor(Math.random() * 1000);
    this.buffering = false;
    this.lastBitrate = 0;  // always start with a valid integer
    this.init();
  }

  generateId() {
    return Math.random().toString(36).substring(7);
  }

  getCurrentBitrate() {
    const idx = this.hls.currentLevel;
    if (idx < 0) return this.lastBitrate;
    const level = this.hls.levels?.[idx];
    if (!level) return this.lastBitrate;
    // bitrate is in bps, convert to kbps and cache it
    this.lastBitrate = Math.round((level.bitrate || 0) / 1000);
    return this.lastBitrate;
  }

  sendEvent(eventType, extra = {}) {
    const payload = {
      event_id: this.generateId(),
      timestamp: Date.now(),
      viewer_id: this.viewerId,
      session_id: this.sessionId,
      event_type: eventType,
      video_position: Math.round(this.video.currentTime * 1000) / 1000,
      ...extra,
      bitrate: this.getCurrentBitrate()
    };

    console.log("Sending event:", payload);

    fetch("http://localhost:8000/telemetry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  }

  init() {
    // Cache bitrate as soon as HLS knows the levels
    this.hls.on(Hls.Events.MANIFEST_PARSED, () => {
      this.getCurrentBitrate();
    });

    // Update cached bitrate on every level switch
    this.hls.on(Hls.Events.LEVEL_SWITCHED, (_, data) => {
      this.getCurrentBitrate();  // updates this.lastBitrate
      this.sendEvent("QUALITY_CHANGE", { level: data.level });
    });

    this.video.addEventListener("waiting", () => {
      if (!this.buffering) {
        this.buffering = true;
        this.sendEvent("BUFFER_START");
      }
    });

    this.video.addEventListener("playing", () => {
      if (this.buffering) {
        this.buffering = false;
        this.sendEvent("BUFFER_END");
      }
    });

    setInterval(() => {
      this.sendEvent("HEARTBEAT");
    }, 5000);
  }
}

const waitForHls = setInterval(() => {
  if (window.hls && document.getElementById("video")) {
    clearInterval(waitForHls);
    new TelemetryCollector(window.hls, document.getElementById("video"));
  }
}, 500);