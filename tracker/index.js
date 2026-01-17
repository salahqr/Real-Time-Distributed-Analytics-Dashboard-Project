(function () {
  'use strict';

  class AnalyticsTracker {
    constructor() {
      // Get configuration from script tag
      this.config = this.getConfig();

      // Initialize tracking data
      this.pageStart = Date.now();
      this.sessionId = this.getSessionId();
      this.userId = this.getUserId();

      this.eventBuffer = {
        clickCount: 0,
        linkClick: [],
        videoEvent: [],
        mouse_click: [],
        mouse_movement: [],
        formSubmission: [],
        scroll_events: [],
        form_interactions: [],
      };

      this.milestones = [25, 50, 75, 100];
      this.lastMove = 0;
      this.sendQueue = [];
      this.isOnline = navigator.onLine;

      this.init();
    }

    getConfig() {
      const script =
        document.currentScript ||
        document.querySelector('script[data-endpoint]') ||
        document.querySelector('script[src*="analytics"]');

      return {
        endpoint: script?.getAttribute('data-endpoint') || '/analytics',
        trackingId: script?.getAttribute('data-tracking-id') || 'default',
        batchSize: parseInt(script?.getAttribute('data-batch-size')) || 10,
        sendInterval: parseInt(script?.getAttribute('data-interval')) || 7000,
        enableDebug: script?.getAttribute('data-debug') === 'true',
      };
    }

    init() {
      this.log('Analytics tracker initialized with config:', this.config);

      // Wait for DOM to be ready
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => this.start());
      } else {
        this.start();
      }
    }

    start() {
      this.sendFirstTimeData();
      this.setupEventListeners();
      this.startPeriodicSending();
      this.setupNetworkListeners();
    }

    log(...args) {
      if (this.config.enableDebug) {
        console.log('[Analytics]', ...args);
      }
    }

    getSessionId() {
      let sid = sessionStorage.getItem('analytics_session_id');
      if (!sid) {
        sid = this.generateId();
        sessionStorage.setItem('analytics_session_id', sid);
      }
      return sid;
    }

    getUserId() {
      let uid = localStorage.getItem('analytics_user_id');
      if (!uid) {
        uid = this.generateId();
        localStorage.setItem('analytics_user_id', uid);
      }
      return uid;
    }

    generateId() {
      return 'xxxx-xxxx-xxxx'.replace(/[x]/g, () =>
        ((Math.random() * 16) | 0).toString(16)
      );
    }

    async sendFirstTimeData() {
      const firstTimeData = {
        url: window.document.URL,
        referrer: window.document.referrer,
        title: window.document.title,
        screen_resolution: {
          width: screen.width,
          height: screen.height,
          available_width: screen.availWidth,
          available_height: screen.availHeight,
          color_depth: screen.colorDepth,
        },
        viewport: {
          width: window.innerWidth,
          height: window.innerHeight,
        },
        operating_system: window.navigator.platform,
        browser: window.navigator.userAgent,
        language: window.navigator.language,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        device_type: this.getDeviceType(),
        location: await this.getLocation(),
        session_id: this.sessionId,
        user_id: this.userId,
        tracking_id: this.config.trackingId,
        timestamp: new Date().toISOString(),
        page_load_time: Date.now() - performance.navigationStart,
      };

      // Add network information if available
      if ('connection' in window.navigator) {
        const connection = window.navigator.connection;
        firstTimeData.network = {
          effectiveType: connection.effectiveType,
          downlink: connection.downlink,
          rtt: connection.rtt,
          saveData: connection.saveData,
        };
      }

      // Add performance data
      if (window.performance && window.performance.timing) {
        const timing = window.performance.timing;
        firstTimeData.performance = {
          dns_time: timing.domainLookupEnd - timing.domainLookupStart,
          connect_time: timing.connectEnd - timing.connectStart,
          response_time: timing.responseEnd - timing.responseStart,
          dom_load_time:
            timing.domContentLoadedEventEnd - timing.navigationStart,
          page_load_time: timing.loadEventEnd - timing.navigationStart,
        };
      }

      this.sendEvent({
        type: 'page_load',
        data: firstTimeData,
      });

      this.sendEvent({
        type: 'page_view',
        page_url: location.href,
        page_title: document.title,
        session_id: this.sessionId,
        user_id: this.userId,
        ts: Date.now(),
      });
    }

    getDeviceType() {
      const width = window.innerWidth;
      const userAgent = navigator.userAgent.toLowerCase();

      // Check for mobile devices
      if (/mobile|android|iphone|ipod|phone/i.test(userAgent)) {
        return 'Mobile';
      }

      // Check for tablets
      if (
        /tablet|ipad|kindle|silk/i.test(userAgent) ||
        ('ontouchstart' in window && width >= 768 && width < 1024)
      ) {
        return 'Tablet';
      }

      // Check touch capability
      if ('ontouchstart' in window || navigator.maxTouchPoints > 0) {
        if (width < 768) return 'Mobile';
        if (width < 1024) return 'Tablet';
      }

      return 'Desktop';
    }

    async getLocation() {
      try {
        const response = await fetch('https://ipapi.co/json/');
        if (!response.ok) throw new Error('Network response was not ok');

        const data = await response.json();
        return {
          country: data.country_name,
          country_code: data.country_code,
          city: data.city,
          region: data.region,
          postal: data.postal,
          ip: data.ip,
          latitude: data.latitude,
          longitude: data.longitude,
        };
      } catch (err) {
        this.log('GeoIP error:', err);
        return { error: 'Unable to fetch location' };
      }
    }

    setupEventListeners() {
      // Form submission tracking
      document.addEventListener('submit', (event) => {
        const form = event.target;
        const formData = new FormData(form);

        // Don't capture sensitive data, only structure
        const formInfo = {
          form_id: form.id || 'unknown',
          form_name: form.name || 'unknown',
          action: form.action || window.location.href,
          method: form.method || 'GET',
          field_count: form.elements.length,
          has_file_upload: Array.from(form.elements).some(
            (el) => el.type === 'file'
          ),
          timestamp: new Date().toISOString(),
          page_url: window.location.href,
        };

        this.eventBuffer.formSubmission.push(formInfo);

        this.sendEvent({
          type: 'form_submit',
          ...formInfo,
          ts: Date.now(),
        });
      });

      // Enhanced click tracking
      document.addEventListener('click', (e) => {
        this.eventBuffer.clickCount += 1;

        // Check for link clicks
        const a = e.target.closest('a');
        if (a) {
          const linkEvent = {
            type: a.classList.contains('download')
              ? 'file_download'
              : 'link_click',
            url: a.href,
            text: a.textContent?.trim().substring(0, 100) || '',
            file_name: a.href.split('/').pop(),
            is_external: !a.href.startsWith(window.location.origin),
            target: a.target,
            page_url: location.href,
            ts: Date.now(),
          };
          this.eventBuffer.linkClick.push(linkEvent);
        }

        // Button clicks
        const button = e.target.closest('button, [role="button"]');
        if (button) {
          this.sendEvent({
            type: 'button_click',
            button_text: button.textContent?.trim().substring(0, 50) || '',
            button_type: button.type || 'unknown',
            button_id: button.id || 'unknown',
            page_url: location.href,
            ts: Date.now(),
          });
        }

        // General click tracking
        const clickPayload = {
          type: 'mouse_click',
          x: e.clientX,
          y: e.clientY,
          element: e.target.tagName.toLowerCase(),
          element_id: e.target.id || null,
          element_class: e.target.className || null,
          page_url: location.href,
          ts: Date.now(),
        };
        this.eventBuffer.mouse_click.push(clickPayload);
      });

      // Enhanced mouse movement tracking (with sampling)
      let moveCount = 0;
      document.addEventListener('mousemove', (e) => {
        moveCount++;
        // Sample only every 10th movement and throttle by time
        if (moveCount % 10 === 0 && Date.now() - this.lastMove > 500) {
          this.lastMove = Date.now();
          const payload = {
            type: 'mouse_move',
            x: Math.round(e.clientX),
            y: Math.round(e.clientY),
            page_url: location.href,
            ts: this.lastMove,
          };
          this.eventBuffer.mouse_movement.push(payload);
        }
      });

      // Video tracking for all videos
      this.setupVideoTracking();

      // Enhanced scroll tracking
      let scrollTimeout;
      window.addEventListener('scroll', () => {
        clearTimeout(scrollTimeout);
        scrollTimeout = setTimeout(() => {
          const docHeight =
            document.documentElement.scrollHeight - window.innerHeight;
          const scrollTop = window.scrollY;
          const percent = Math.round((scrollTop / docHeight) * 100);

          // Track milestones
          this.milestones = this.milestones.filter((m) => {
            if (percent >= m) {
              this.sendEvent({
                type: 'scroll_depth',
                depth: m,
                page_url: location.href,
                ts: Date.now(),
              });
              return false;
            }
            return true;
          });

          // Track scroll position
          this.eventBuffer.scroll_events.push({
            scroll_percent: percent,
            scroll_top: scrollTop,
            ts: Date.now(),
          });
        }, 100);
      });

      // Form field interactions
      document.addEventListener('focusin', (e) => {
        if (this.isFormElement(e.target)) {
          const fieldData = {
            type: 'form_focus',
            field_name: e.target.name || e.target.id || 'unknown',
            field_type: e.target.type || e.target.tagName.toLowerCase(),
            page_url: location.href,
            ts: Date.now(),
          };

          this.eventBuffer.form_interactions.push(fieldData);
          this.sendEvent(fieldData);
        }
      });

      document.addEventListener('input', (e) => {
        if (this.isFormElement(e.target)) {
          this.sendEvent({
            type: 'form_input',
            field_name: e.target.name || e.target.id || 'unknown',
            field_type: e.target.type || e.target.tagName.toLowerCase(),
            value_length: e.target.value?.length || 0,
            page_url: location.href,
            ts: Date.now(),
          });
        }
      });

      // Page visibility changes
      document.addEventListener('visibilitychange', () => {
        this.sendEvent({
          type: document.hidden ? 'page_hidden' : 'page_visible',
          page_url: location.href,
          ts: Date.now(),
        });
      });

      // Page unload tracking
      window.addEventListener('beforeunload', () => {
        const duration = Date.now() - this.pageStart;
        const unloadData = {
          type: 'page_unload',
          page_url: location.href,
          duration_ms: duration,
          scroll_depth_max: Math.max(
            ...(this.eventBuffer.scroll_events.map((s) => s.scroll_percent) || [
              0,
            ])
          ),
          click_count: this.eventBuffer.clickCount,
          ts: Date.now(),
        };

        // Use sendBeacon for reliable unload tracking
        this.sendEventImmediate(unloadData);
      });
    }

    setupVideoTracking() {
      // Track all existing videos
      document
        .querySelectorAll('video')
        .forEach((video) => this.trackVideo(video));

      // Track dynamically added videos
      const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
          mutation.addedNodes.forEach((node) => {
            if (node.nodeType === Node.ELEMENT_NODE) {
              if (node.tagName === 'VIDEO') {
                this.trackVideo(node);
              } else {
                node
                  .querySelectorAll?.('video')
                  .forEach((video) => this.trackVideo(video));
              }
            }
          });
        });
      });

      observer.observe(document.body, { childList: true, subtree: true });
    }

    trackVideo(video) {
      if (video._analyticsTracked) return;
      video._analyticsTracked = true;

      const videoData = {
        video_src: video.src || video.currentSrc || 'unknown',
        video_duration: video.duration || 0,
      };

      video.addEventListener('play', () => {
        this.eventBuffer.videoEvent.push({
          ...videoData,
          type: 'play',
          current_time: video.currentTime,
          ts: Date.now(),
        });
      });

      video.addEventListener('pause', () => {
        this.eventBuffer.videoEvent.push({
          ...videoData,
          type: 'pause',
          current_time: video.currentTime,
          ts: Date.now(),
        });
      });

      video.addEventListener('ended', () => {
        this.eventBuffer.videoEvent.push({
          ...videoData,
          type: 'complete',
          ts: Date.now(),
        });
      });

      video.addEventListener('timeupdate', () => {
        if (!video.duration) return;

        const percent = (video.currentTime / video.duration) * 100;

        if (percent >= 25 && !video._q25) {
          this.eventBuffer.videoEvent.push({
            ...videoData,
            type: 'progress_25',
            current_time: video.currentTime,
            ts: Date.now(),
          });
          video._q25 = true;
        }
        if (percent >= 50 && !video._q50) {
          this.eventBuffer.videoEvent.push({
            ...videoData,
            type: 'progress_50',
            current_time: video.currentTime,
            ts: Date.now(),
          });
          video._q50 = true;
        }
        if (percent >= 75 && !video._q75) {
          this.eventBuffer.videoEvent.push({
            ...videoData,
            type: 'progress_75',
            current_time: video.currentTime,
            ts: Date.now(),
          });
          video._q75 = true;
        }
      });
    }

    isFormElement(element) {
      return ['INPUT', 'TEXTAREA', 'SELECT'].includes(element.tagName);
    }

    setupNetworkListeners() {
      window.addEventListener('online', () => {
        this.isOnline = true;
        this.processSendQueue();
      });

      window.addEventListener('offline', () => {
        this.isOnline = false;
      });
    }

    // Send event data
    sendEvent(eventData) {
      // Add common data to all events
      eventData.session_id = this.sessionId;
      eventData.user_id = this.userId;
      eventData.tracking_id = this.config.trackingId;
      eventData.url = window.location.href;

      this.log('Event:', eventData);

      if (this.isOnline) {
        this.sendToServer(eventData);
      } else {
        this.sendQueue.push(eventData);
      }
    }

    sendEventImmediate(eventData) {
      eventData.session_id = this.sessionId;
      eventData.user_id = this.userId;
      eventData.tracking_id = this.config.trackingId;

      // Use sendBeacon for immediate sending (like page unload)
      if (navigator.sendBeacon) {
        const blob = new Blob([JSON.stringify(eventData)], {
          type: 'application/json',
        });
        navigator.sendBeacon(this.config.endpoint, blob);
      } else {
        this.sendToServer(eventData);
      }
    }

    async sendToServer(eventData) {
      try {
        const response = await fetch(this.config.endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(eventData),
          keepalive: true,
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        this.log('Event sent successfully');
      } catch (err) {
        this.log('Error sending event:', err);
        // Add to queue for retry
        this.sendQueue.push(eventData);
      }
    }

    processSendQueue() {
      if (this.sendQueue.length === 0) return;

      const batch = this.sendQueue.splice(0, this.config.batchSize);

      Promise.all(batch.map((event) => this.sendToServer(event)))
        .then(() => {
          if (this.sendQueue.length > 0) {
            setTimeout(() => this.processSendQueue(), 1000);
          }
        })
        .catch((err) => {
          this.log('Batch send failed:', err);
          // Re-add failed events to queue
          this.sendQueue.unshift(...batch);
        });
    }

    startPeriodicSending() {
      setInterval(() => {
        if (this.hasEventData()) {
          this.sendPeriodicData();
        }
      }, this.config.sendInterval);
    }

    hasEventData() {
      return Object.values(this.eventBuffer).some((buffer) =>
        Array.isArray(buffer) ? buffer.length > 0 : buffer !== null
      );
    }

    sendPeriodicData() {
      const periodicData = {
        type: 'periodic_events',
        clickCount: this.eventBuffer.clickCount,
        linkClicks: [...this.eventBuffer.linkClick],
        videoEvents: [...this.eventBuffer.videoEvent],
        mouseClicks: [...this.eventBuffer.mouse_click],
        mouseMovements: [...this.eventBuffer.mouse_movement],
        formSubmissions: [...this.eventBuffer.formSubmission],
        scrollEvents: [...this.eventBuffer.scroll_events],
        formInteractions: [...this.eventBuffer.form_interactions],
        session_id: this.sessionId,
        user_id: this.userId,
        ts: Date.now(),
      };

      this.sendEvent(periodicData);
      this.clearEventBuffers();
    }

    clearEventBuffers() {
      this.eventBuffer.linkClick = [];
      this.eventBuffer.videoEvent = [];
      this.eventBuffer.mouse_click = [];
      this.eventBuffer.mouse_movement = [];
      this.eventBuffer.formSubmission = [];
      this.eventBuffer.scroll_events = [];
      this.eventBuffer.form_interactions = [];
    }

    // E-commerce tracking methods
    trackProductView(productId, productName, price, category) {
      this.sendEvent({
        type: 'product_view',
        product_id: productId,
        product_name: productName,
        price: price,
        category: category,
        page_url: location.href,
        ts: Date.now(),
      });
    }

    trackPurchase(orderId, items, total, currency = 'USD') {
      this.sendEvent({
        type: 'purchase',
        order_id: orderId,
        items: items,
        total: total,
        currency: currency,
        ts: Date.now(),
      });
    }

    trackCartAdd(productId, productName, price, quantity = 1) {
      this.sendEvent({
        type: 'cart_add',
        product_id: productId,
        product_name: productName,
        price: price,
        quantity: quantity,
        ts: Date.now(),
      });
    }

    trackCartRemove(productId) {
      this.sendEvent({
        type: 'cart_remove',
        product_id: productId,
        ts: Date.now(),
      });
    }

    trackCheckoutStep(step, stepName) {
      this.sendEvent({
        type: 'checkout_step',
        step: step,
        step_name: stepName,
        ts: Date.now(),
      });
    }

    // Custom event tracking
    track(eventName, properties = {}) {
      this.sendEvent({
        type: 'custom_event',
        event_name: eventName,
        properties: properties,
        ts: Date.now(),
      });
    }
  }

  // Initialize analytics when script loads
  const analytics = new AnalyticsTracker();

  // Make analytics available globally for custom tracking
  window.analytics = {
    track: (eventName, properties) => analytics.track(eventName, properties),
    trackProductView: (productId, productName, price, category) =>
      analytics.trackProductView(productId, productName, price, category),
    trackPurchase: (orderId, items, total, currency) =>
      analytics.trackPurchase(orderId, items, total, currency),
    trackCartAdd: (productId, productName, price, quantity) =>
      analytics.trackCartAdd(productId, productName, price, quantity),
    trackCartRemove: (productId) => analytics.trackCartRemove(productId),
    trackCheckoutStep: (step, stepName) =>
      analytics.trackCheckoutStep(step, stepName),
  };
})();
