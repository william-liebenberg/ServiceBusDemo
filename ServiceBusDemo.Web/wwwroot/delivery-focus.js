let stopWatching;

function watchFocusedCard(target) {
    stopWatching?.();
    const card = target?.closest?.(".deliveries .delivery-card");
    if (!card) {
        return;
    }

    const clearButton = card.closest(".deliveries").querySelector("button");
    const observer = new MutationObserver(() => {
        if (card.isConnected) {
            return;
        }

        stop();
        // Removal can reset focus to the body without firing a blur event.
        // Keep any focus the user has already moved, and do nothing on navigation away.
        if (clearButton?.isConnected && document.activeElement === document.body) {
            clearButton.focus();
        }
    });

    function onFocusOut() {
        // Some browsers fire focusout before removing the card. Let that DOM update finish.
        queueMicrotask(() => {
            if (card.isConnected && !card.contains(document.activeElement)) {
                stop();
            }
        });
    }

    function stop() {
        observer.disconnect();
        card.removeEventListener("focusout", onFocusOut);
        if (stopWatching === stop) {
            stopWatching = undefined;
        }
    }

    stopWatching = stop;
    card.addEventListener("focusout", onFocusOut);
    observer.observe(document.body, { childList: true, subtree: true });
}

document.addEventListener("focusin", event => watchFocusedCard(event.target));
watchFocusedCard(document.activeElement);
