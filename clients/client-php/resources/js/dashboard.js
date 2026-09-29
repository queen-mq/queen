/*
 * Queen supervisor dashboard: partial auto-refresh with a pause control.
 *
 * Served same-origin, content-hashed and with Subresource Integrity; the
 * Content Security Policy allows no inline code. The page works without this
 * file: a <noscript> meta refresh reloads it whole.
 *
 * Every refresh fetches the current page and swaps only the header and the
 * main region, so scroll position and the sidebar stay put. It pauses while the tab is hidden, while the reader is selecting text
 * or has focus on a control that would be replaced, and stops for good when
 * the response is no longer this dashboard (an expired session redirects to
 * the application's login page).
 */
(function () {
    'use strict';

    var STORAGE_KEY = 'queen-dashboard:auto-refresh';
    var body = document.body;
    if (!body.hasAttribute('data-refresh-seconds')) {
        // A page with nothing to refresh, such as a failed-job detail.
        return;
    }
    var seconds = Math.max(2, parseInt(body.getAttribute('data-refresh-seconds'), 10) || 5);
    var enabled = readPreference();
    var stopped = false;
    var timer = null;
    var inFlight = false;

    function readPreference() {
        try {
            return window.localStorage.getItem(STORAGE_KEY) !== 'off';
        } catch (error) {
            return true;
        }
    }

    function writePreference(value) {
        try {
            window.localStorage.setItem(STORAGE_KEY, value ? 'on' : 'off');
        } catch (error) {
            // Private browsing may refuse storage; the choice then lasts for this page only.
        }
    }

    function header() {
        return document.querySelector('.topbar');
    }

    function main() {
        return document.getElementById('main-content');
    }

    function renderControls() {
        var toggle = document.querySelector('[data-refresh-toggle]');
        var state = document.querySelector('[data-refresh-state]');
        if (toggle) {
            toggle.hidden = stopped;
            toggle.setAttribute('aria-pressed', enabled ? 'false' : 'true');
            toggle.textContent = enabled ? 'Pause auto-refresh' : 'Resume auto-refresh';
        }
        if (state) {
            state.textContent = stopped
                ? 'Auto-refresh stopped: reload the page'
                : (enabled ? 'Auto-refreshes every ' + seconds + ' seconds' : 'Auto-refresh paused');
        }
    }

    function schedule() {
        window.clearTimeout(timer);
        if (enabled && !stopped && !document.hidden) {
            timer = window.setTimeout(refresh, seconds * 1000);
        }
    }

    function readerIsBusy() {
        var selection = window.getSelection ? window.getSelection() : null;
        if (selection && !selection.isCollapsed) {
            return true;
        }
        var active = document.activeElement;
        if (!active || active === body || active.hasAttribute('data-refresh-toggle')) {
            return false;
        }
        var top = header();
        var content = main();

        return (top !== null && top.contains(active)) || (content !== null && content.contains(active));
    }

    function stop() {
        stopped = true;
        window.clearTimeout(timer);
        renderControls();
    }

    function swap(next) {
        var nextHeader = next.querySelector('.topbar');
        var nextMain = next.getElementById('main-content');
        var currentHeader = header();
        var currentMain = main();
        if (!nextHeader || !nextMain || !currentHeader || !currentMain) {
            return false;
        }
        var toggleHadFocus = document.activeElement !== null
            && document.activeElement.hasAttribute('data-refresh-toggle');
        // Emptying the region shortens the page for a moment and the browser
        // clamps the scroll offset; put the reader back where they were.
        var scrollX = window.scrollX;
        var scrollY = window.scrollY;

        currentHeader.replaceWith(document.importNode(nextHeader, true));
        var children = [];
        for (var i = 0; i < nextMain.childNodes.length; i++) {
            children.push(document.importNode(nextMain.childNodes[i], true));
        }
        currentMain.replaceChildren.apply(currentMain, children);
        window.scrollTo(scrollX, scrollY);

        renderControls();
        if (toggleHadFocus) {
            var toggle = document.querySelector('[data-refresh-toggle]');
            if (toggle) {
                toggle.focus();
            }
        }

        return true;
    }

    function refresh() {
        if (inFlight || stopped || !enabled) {
            return;
        }
        if (readerIsBusy()) {
            schedule();
            return;
        }
        inFlight = true;
        var url = new URL(window.location.href);
        url.hash = '';

        window.fetch(url.toString(), {
            credentials: 'same-origin',
            cache: 'no-store',
            headers: { Accept: 'text/html' }
        }).then(function (response) {
            if (!response.ok || response.redirected) {
                stop();
                return null;
            }
            return response.text();
        }).then(function (html) {
            if (html === null) {
                return;
            }
            var next = new DOMParser().parseFromString(html, 'text/html');
            if (!swap(next)) {
                stop();
            }
        }).catch(function () {
            // A network error is transient: keep the current view and try
            // again at the next interval.
        }).then(function () {
            inFlight = false;
            schedule();
        });
    }

    document.addEventListener('click', function (event) {
        if (event.target && event.target.closest && event.target.closest('[data-refresh-toggle]')) {
            enabled = !enabled;
            writePreference(enabled);
            renderControls();
            if (enabled) {
                refresh();
            } else {
                window.clearTimeout(timer);
            }
        }
    });

    document.addEventListener('visibilitychange', function () {
        if (document.hidden) {
            window.clearTimeout(timer);
        } else {
            refresh();
        }
    });

    renderControls();
    schedule();
}());
