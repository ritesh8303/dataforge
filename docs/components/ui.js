/* DataForge — lightweight UI enhancements (scroll reveal, mobile filter drawer) */
(function () {
  'use strict';

  /* Scroll reveal */
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

  /* Mobile filter drawer (job board) */
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
})();
