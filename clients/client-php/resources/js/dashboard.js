/*
 * Queen supervisor dashboard: partial auto-refresh with a pause control, and
 * the failed-job drawer.
 *
 * Served same-origin, content-hashed and with Subresource Integrity; the
 * Content Security Policy allows no inline code. The page works without this
 * file: a <noscript> meta refresh reloads it whole.
 *
 * Every refresh fetches the current page and swaps only the header and the
 * main region, so scroll position and the sidebar stay put. A focused link
 * keeps its focus across the swap. It pauses while the tab is hidden, while
 * the drawer is open, while the reader is selecting text or has focus on a
 * control that would be replaced, and stops for good when
 * the response is no longer this dashboard (an expired session redirects to
 * the application's login page).
 *
 * A link marked data-drawer opens its page's #failed-job section in a modal
 * <dialog> instead of navigating. Without this file, or without <dialog>, the
 * same link opens the detail page itself.
 *
 * A button marked data-copy copies the <pre> of its .detail-block. The
 * buttons stay hidden until this file runs.
 */
(function () {
    'use strict';

    document.documentElement.classList.add('copy-enabled');

    // Without the Clipboard API (an http:// origin is not a secure context),
    // copy through a selected, invisible textarea next to the button: inside
    // a modal drawer, anything outside it is inert and cannot be selected.
    function copyText(text, near) {
        if (navigator.clipboard && window.isSecureContext) {
            return navigator.clipboard.writeText(text);
        }
        return new Promise(function (resolve, reject) {
            var buffer = document.createElement('textarea');
            buffer.className = 'copy-buffer';
            buffer.setAttribute('readonly', '');
            buffer.setAttribute('aria-hidden', 'true');
            buffer.value = text;
            near.parentNode.appendChild(buffer);
            buffer.select();
            var copied = false;
            try {
                copied = document.execCommand('copy');
            } catch (error) {
                copied = false;
            }
            buffer.remove();
            near.focus();
            if (copied) {
                resolve();
            } else {
                reject(new Error('copy refused'));
            }
        });
    }

    function announce(button, copied) {
        var section = button.closest('section');
        var status = section ? section.querySelector('[data-copy-status]') : null;
        var label = button.getAttribute('aria-label') || 'Text';
        if (status) {
            status.textContent = copied
                ? label.replace(/^Copy the /, '').replace(/^./, function (c) { return c.toUpperCase(); }) + ' copied to the clipboard.'
                : 'Copy failed: the text is selected, press Ctrl+C or Cmd+C.';
        }
        button.textContent = copied ? 'Copied' : 'Select';
        if (copied) {
            button.setAttribute('data-copied', '');
        }
        window.clearTimeout(button.copyTimer);
        button.copyTimer = window.setTimeout(function () {
            button.textContent = 'Copy';
            button.removeAttribute('data-copied');
        }, 2000);
    }

    function selectContents(node) {
        var range = document.createRange();
        range.selectNodeContents(node);
        var selection = window.getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
    }

    document.addEventListener('click', function (event) {
        var button = event.target && event.target.closest ? event.target.closest('button[data-copy]') : null;
        if (button === null) {
            return;
        }
        var block = button.closest('.detail-block');
        var source = block ? block.querySelector('pre') : null;
        if (source === null) {
            return;
        }
        copyText(source.textContent, button).then(function () {
            announce(button, true);
        }, function () {
            // Leave the text selected so the reader can copy it by hand.
            selectContents(source);
            announce(button, false);
        });
    });
}());

(function () {
    'use strict';

    var drawer = null;
    var body = null;
    var pageLink = null;
    var opener = null;
    var request = 0;

    function element(tag, className, text) {
        var node = document.createElement(tag);
        if (className) {
            node.className = className;
        }
        if (text) {
            node.textContent = text;
        }
        return node;
    }

    function build() {
        drawer = element('dialog', 'drawer');
        drawer.setAttribute('aria-label', 'Failed job');
        var bar = element('div', 'drawer-bar');
        pageLink = element('a', 'button', 'Open as page');
        var close = element('button', 'button', 'Close');
        close.type = 'button';
        close.addEventListener('click', function () {
            drawer.close();
        });
        bar.appendChild(pageLink);
        bar.appendChild(close);
        body = element('div', 'drawer-body');
        drawer.appendChild(bar);
        drawer.appendChild(body);
        // A click on the backdrop lands on the dialog element itself.
        drawer.addEventListener('click', function (event) {
            if (event.target === drawer) {
                drawer.close();
            }
        });
        drawer.addEventListener('close', function () {
            request++;
            if (opener !== null && document.contains(opener)) {
                opener.focus();
            }
            opener = null;
        });
        // Outside #main-content, so an automatic refresh never replaces it.
        document.body.appendChild(drawer);
    }

    function status(text) {
        drawer.removeAttribute('aria-labelledby');
        drawer.setAttribute('aria-label', 'Failed job');
        body.replaceChildren(element('p', 'drawer-status', text));
    }

    function open(link) {
        if (drawer === null) {
            build();
        }
        var current = ++request;
        opener = link;
        pageLink.href = link.href;
        status('Loading…');
        if (!drawer.open) {
            drawer.showModal();
        }

        window.fetch(link.href, {
            credentials: 'same-origin',
            cache: 'no-store',
            headers: { Accept: 'text/html' }
        }).then(function (response) {
            if (current !== request) {
                return null;
            }
            if (response.redirected) {
                // The session expired: let the application handle the page.
                window.location.assign(link.href);
                return null;
            }
            if (response.status === 404) {
                status('This job is no longer in the failed-job store.');
                return null;
            }
            if (!response.ok) {
                status('The job could not be loaded. Open it as a page instead.');
                return null;
            }
            return response.text();
        }).then(function (html) {
            if (html === null || current !== request) {
                return;
            }
            var section = new DOMParser().parseFromString(html, 'text/html').getElementById('failed-job');
            if (!section) {
                status('The job could not be loaded. Open it as a page instead.');
                return;
            }
            body.replaceChildren(document.importNode(section, true));
            drawer.removeAttribute('aria-label');
            drawer.setAttribute('aria-labelledby', 'failed-job-title');
        }).catch(function () {
            if (current === request) {
                status('The job could not be loaded. Open it as a page instead.');
            }
        });
    }

    if (typeof window.HTMLDialogElement !== 'function' || !window.fetch || !window.DOMParser) {
        return;
    }
    document.addEventListener('click', function (event) {
        if (event.defaultPrevented || event.button !== 0
            || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
            return;
        }
        var link = event.target && event.target.closest ? event.target.closest('a[data-drawer]') : null;
        if (link === null) {
            return;
        }
        event.preventDefault();
        open(link);
    });
}());

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
        if (document.querySelector('dialog.drawer[open]') !== null) {
            return true;
        }
        var selection = window.getSelection ? window.getSelection() : null;
        if (selection && !selection.isCollapsed) {
            return true;
        }
        var active = document.activeElement;
        // A focused link is put back after the swap (see swap()); only a
        // focused control would lose the reader's place.
        if (!active || active === body || active.hasAttribute('data-refresh-toggle')
            || (active.tagName === 'A' && active.hasAttribute('href'))) {
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
        var focusedHref = document.activeElement !== null
            && document.activeElement.tagName === 'A'
            && (currentHeader.contains(document.activeElement) || currentMain.contains(document.activeElement))
            ? document.activeElement.getAttribute('href')
            : null;
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
        } else if (focusedHref !== null && window.CSS && CSS.escape) {
            var link = document.querySelector('.topbar a[href="' + CSS.escape(focusedHref) + '"], #main-content a[href="' + CSS.escape(focusedHref) + '"]');
            if (link) {
                link.focus({ preventScroll: true });
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
