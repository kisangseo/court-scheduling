(function attachSlotCountHelper(globalScope) {
    function toNumber(value, fallback = 0) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }

    function countNormalizedSlots(slots) {
        const safeSlots = Array.isArray(slots) ? slots : [];
        const totals = { vacant: 0, filled: 0, open: 0, totalRequired: 0 };

        safeSlots.forEach((slot) => {
            if (!slot || slot.countable === false) return;

            const requiredDeputies = Math.max(0, toNumber(slot.requiredDeputies, 0));
            totals.totalRequired += requiredDeputies;

            const state = (slot.state || '').trim().toLowerCase();
            if (state === 'vacant') totals.vacant += 1;
            else if (state === 'filled') totals.filled += 1;
            else if (state === 'open') totals.open += 1;
        });

        return totals;
    }

    globalScope.SlotCountHelper = {
        countNormalizedSlots
    };
})(window);
