/* DataForge — neural scene, 3D tilt, scroll reveal, drawers */
(function () {
  'use strict';

  var reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ── Neural scene ── */
  function injectScene() {
    if (document.querySelector('.df-scene')) return;
    var scene = document.createElement('div');
    scene.className = 'df-scene';
    scene.setAttribute('aria-hidden', 'true');
    scene.innerHTML =
      '<canvas class="df-scene-canvas"></canvas>' +
      '<div class="df-scene-orbs">' +
        '<div class="df-orb df-orb-1"></div>' +
        '<div class="df-orb df-orb-2"></div>' +
        '<div class="df-orb df-orb-3"></div>' +
      '</div>' +
      '<div class="df-scene-grid"></div>' +
      '<div class="df-scene-vignette"></div>';
    var noise = document.createElement('div');
    noise.className = 'df-noise';
    noise.setAttribute('aria-hidden', 'true');
    document.body.insertBefore(scene, document.body.firstChild);
    document.body.insertBefore(noise, scene.nextSibling);
    if (!reduceMotion) initParticles(scene.querySelector('.df-scene-canvas'));
  }

  function initParticles(canvas) {
    if (!canvas) return;
    var ctx = canvas.getContext('2d');
    var nodes = [];
    var mouse = { x: -9999, y: -9999 };
    var raf;

    function resize() {
      var dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = window.innerWidth * dpr;
      canvas.height = window.innerHeight * dpr;
      canvas.style.width = window.innerWidth + 'px';
      canvas.style.height = window.innerHeight + 'px';
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      var count = Math.max(28, Math.min(64, Math.floor((window.innerWidth * window.innerHeight) / 22000)));
      nodes = [];
      for (var i = 0; i < count; i++) {
        nodes.push({
          x: Math.random() * window.innerWidth,
          y: Math.random() * window.innerHeight,
          vx: (Math.random() - 0.5) * 0.35,
          vy: (Math.random() - 0.5) * 0.35,
          r: Math.random() * 1.6 + 0.5
        });
      }
    }

    function tick() {
      ctx.clearRect(0, 0, window.innerWidth, window.innerHeight);
      for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        n.x += n.vx;
        n.y += n.vy;
        if (n.x < 0 || n.x > window.innerWidth) n.vx *= -1;
        if (n.y < 0 || n.y > window.innerHeight) n.vy *= -1;
        var dxm = n.x - mouse.x;
        var dym = n.y - mouse.y;
        var dm = Math.sqrt(dxm * dxm + dym * dym);
        if (dm < 140) {
          n.x += dxm / dm * 0.35;
          n.y += dym / dm * 0.35;
        }
      }
      for (var a = 0; a < nodes.length; a++) {
        for (var b = a + 1; b < nodes.length; b++) {
          var dx = nodes[a].x - nodes[b].x;
          var dy = nodes[a].y - nodes[b].y;
          var dist = Math.sqrt(dx * dx + dy * dy);
          if (dist < 130) {
            var alpha = (1 - dist / 130) * 0.28;
            ctx.strokeStyle = a % 2 === 0
              ? 'rgba(34,211,238,' + alpha + ')'
              : 'rgba(167,139,250,' + alpha + ')';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(nodes[a].x, nodes[a].y);
            ctx.lineTo(nodes[b].x, nodes[b].y);
            ctx.stroke();
          }
        }
      }
      for (var p = 0; p < nodes.length; p++) {
        ctx.fillStyle = p % 3 === 0 ? 'rgba(34,211,238,0.7)' : (p % 3 === 1 ? 'rgba(167,139,250,0.65)' : 'rgba(244,114,182,0.5)');
        ctx.beginPath();
        ctx.arc(nodes[p].x, nodes[p].y, nodes[p].r, 0, Math.PI * 2);
        ctx.fill();
      }
      raf = requestAnimationFrame(tick);
    }

    resize();
    window.addEventListener('resize', resize);
    window.addEventListener('pointermove', function (e) {
      mouse.x = e.clientX;
      mouse.y = e.clientY;
    });
    tick();
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) cancelAnimationFrame(raf);
      else tick();
    });
  }

  /* ── Pointer spotlight + 3D tilt ── */
  function initPointerFX() {
    var root = document.documentElement;
    window.addEventListener('pointermove', function (e) {
      root.style.setProperty('--df-mx', e.clientX + 'px');
      root.style.setProperty('--df-my', e.clientY + 'px');
    });

    if (reduceMotion || window.matchMedia('(pointer: coarse)').matches) return;

    var selectors = '.df-card, .job-card, .df-kpi-card, .wizard-card, .df-stat-glass, .hero-preview-frame';
    var tilted = null;
    document.addEventListener('pointermove', function (e) {
      var el = e.target.closest(selectors);
      if (tilted && tilted !== el) tilted.style.transform = '';
      tilted = el || null;
      if (!el) return;
      var rect = el.getBoundingClientRect();
      var dx = (e.clientX - (rect.left + rect.width / 2)) / (rect.width / 2);
      var dy = (e.clientY - (rect.top + rect.height / 2)) / (rect.height / 2);
      var rx = Math.max(-9, Math.min(9, -dy * 8));
      var ry = Math.max(-9, Math.min(9, dx * 8));
      el.style.transform = 'perspective(900px) rotateX(' + rx + 'deg) rotateY(' + ry + 'deg) translateY(-4px)';
    });
    document.addEventListener('pointerleave', function () {
      if (tilted) { tilted.style.transform = ''; tilted = null; }
    });
  }

  /* ── Scroll reveal ── */
  var revealEls = document.querySelectorAll('.df-reveal');
  if (revealEls.length && 'IntersectionObserver' in window) {
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add('is-visible');
          observer.unobserve(entry.target);
        }
      });
    }, { threshold: 0.12, rootMargin: '0px 0px -40px 0px' });
    revealEls.forEach(function (el) { observer.observe(el); });
  } else {
    revealEls.forEach(function (el) { el.classList.add('is-visible'); });
  }

  /* ── Mobile nav ── */
  var navbar = document.querySelector('.df-navbar');
  if (navbar && !navbar.querySelector('.df-nav-toggle')) {
    var toggle = document.createElement('button');
    toggle.className = 'df-nav-toggle';
    toggle.type = 'button';
    toggle.setAttribute('aria-label', 'Menu');
    toggle.textContent = '☰';
    var inner = navbar.querySelector('.df-navbar-inner');
    if (inner) inner.appendChild(toggle);
    toggle.addEventListener('click', function () {
      navbar.classList.toggle('is-open');
    });
  }

  /* ── Mobile filter drawer (job board) ── */
  var sidebar = document.querySelector('.filter-sidebar');
  var backdrop = document.querySelector('.filter-drawer-backdrop');
  var toggleBtn = document.getElementById('filter-drawer-toggle');

  function closeDrawer() {
    if (sidebar) sidebar.classList.remove('open');
    if (backdrop) backdrop.classList.remove('open');
    document.body.style.overflow = '';
  }

  function openDrawer() {
    if (sidebar) sidebar.classList.add('open');
    if (backdrop) backdrop.classList.add('open');
    document.body.style.overflow = 'hidden';
  }

  if (toggleBtn && sidebar) {
    toggleBtn.addEventListener('click', function () {
      if (sidebar.classList.contains('open')) closeDrawer();
      else openDrawer();
    });
  }
  if (backdrop) backdrop.addEventListener('click', closeDrawer);

  injectScene();
  initPointerFX();
})();
