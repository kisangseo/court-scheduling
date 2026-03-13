(function (global) {
    const INACTIVE_COURTROOM_LABELS = new Set([
        'CLOSED',
        'NO DEPUTY',
        'NO DEPUTIES',
        'CIVIL - NO DEPUTIES'
    ]);

    const ONE_DEPUTY_COURTROOM_LABELS = new Set([
        'NEED 1 DEPUTY',
        'CIVIL - 1 DEPUTY',
        'JUVENILE',
        'FAMILY',
        'WAITING TO RECEIVE CASE',
        'OPEN'
    ]);

    function normalizeCourtroomAssignmentLabel(value) {
        return (value || '').trim().toUpperCase();
    }

    function getRequiredDeputiesForCourtroomLabel(value) {
        const normalized = normalizeCourtroomAssignmentLabel(value);
        if (!normalized) return 0;
        if (INACTIVE_COURTROOM_LABELS.has(normalized)) return 0;
        if (normalized === 'NEED 2 DEPUTIES') return 2;
        if (ONE_DEPUTY_COURTROOM_LABELS.has(normalized) || normalized.startsWith('OPEN-')) return 1;
        return 1;
    }

    function isOpenCourtLabel(value) {
        const normalized = normalizeCourtroomAssignmentLabel(value);
        return normalized === 'OPEN' || normalized.startsWith('OPEN-');
    }

    function isInactiveCourtLabel(value) {
        return getRequiredDeputiesForCourtroomLabel(value) === 0;
    }

    function formatCountMetrics(vacant, filled, open, colorFilled) {
        const filledClass = colorFilled || 'metric-green';
        return `<span class="metric-red">Vacant ${vacant}</span> / <span class="${filledClass}">Filled ${filled}</span> / <span class="metric-blue">Open ${open}</span>`;
    }

    function getFixedPostRequirementGroup(slotMeta) {
        const meta = slotMeta || {};
        const courthouse = (meta.courthouse || '').trim().toLowerCase();
        const post = (meta.post || '').trim().toLowerCase();
        const detail = (meta.detail || '').trim().toLowerCase();
        const part = (meta.part || '').trim().toLowerCase();

        if (post === 'transportation') return null;
        if (courthouse === 'mitchell' && post === 'calvert' && part === '0800-3') return null;

        if (courthouse === 'mitchell' && (post === 'jury room' || post === 'st. paul')) {
            return `${courthouse}|jury-stpaul-combined`;
        }

        if (courthouse === 'cummings' && post === 'cummings') {
            if (part === '0800') return `${courthouse}|cummings-0800`;
            if (part.startsWith('0830')) return null;
        }

        return [courthouse, post, detail, part].join('|');
    }


    function shouldCountFixedPostSlotMain(slotMeta) {
        const meta = slotMeta || {};
        const courthouse = (meta.courthouse || '').trim().toLowerCase();
        const post = (meta.post || '').trim().toLowerCase();
        const part = (meta.part || '').trim().toLowerCase();

        if (courthouse === 'mitchell' && post === 'calvert' && part === '0800-3') return false;
        if (courthouse === 'cummings' && post === 'cummings' && part === '0830-2') return false;
        if (courthouse === 'cummings' && post === 'transportation') return false;
        return true;
    }

    function isSecurityPostAssignmentType(value) {
        const normalized = (value || '').trim().toLowerCase();
        return normalized === 'fixed post' || normalized === 'security post';
    }

    function parseAssignedNames(value) {
        const text = (value || '').trim();
        if (!text) return [];
        if (text.includes('||')) return text.split(/\s*\|\|\s*/).filter(Boolean);
        if (text.includes('\n')) return text.split(/\n+/).map(v => v.trim()).filter(Boolean);
        return [text];
    }

    function pickPreferredRow(rows) {
        const rowList = rows || [];
        return rowList.find(r => (r.assigned_member || '').trim()) || rowList[0] || null;
    }

    function getStats(rows) {
        const stats = { vacant: 0, filled: 0, open: 0, total: 0 };
        const fixedPostGroups = new Map();

        (rows || []).forEach(row => {
            const assignmentType = (row.assignment_type || '').trim();
            const assignedCount = parseAssignedNames(row.assigned_member || '').length;

            if (isSecurityPostAssignmentType(assignmentType)) {
                const groupKey = getFixedPostRequirementGroup({
                    courthouse: row.courthouse,
                    post: row.location_group,
                    detail: row.location_detail,
                    part: row.part
                });
                if (!groupKey) return;

                const existingCount = fixedPostGroups.get(groupKey) || 0;
                if (assignedCount > existingCount) fixedPostGroups.set(groupKey, assignedCount);
                return;
            }

            if (assignmentType !== 'Courtroom') return;

            const normalized = normalizeCourtroomAssignmentLabel(row.assignment_notes || '');
            if (isInactiveCourtLabel(normalized)) return;

            if (isOpenCourtLabel(normalized) || isOpenCourtLabel(row.assigned_member || '')) {
                stats.open += 1;
                stats.total += 1;
                return;
            }

            const requiredDeputies = getRequiredDeputiesForCourtroomLabel(normalized);
            if (requiredDeputies <= 0) return;

            stats.filled += Math.min(assignedCount, requiredDeputies);
            stats.vacant += Math.max(requiredDeputies - assignedCount, 0);
            stats.total += requiredDeputies;
        });

        fixedPostGroups.forEach((assignedCount) => {
            if (assignedCount > 0) stats.filled += 1;
            else stats.vacant += 1;
            stats.total += 1;
        });

        return stats;
    }

    global.AssignmentSearchShared = {
        formatCountMetrics,
        normalizeCourtroomAssignmentLabel,
        isOpenCourtLabel,
        isInactiveCourtLabel,
        getRequiredDeputiesForCourtroomLabel,
        getFixedPostRequirementGroup,
        shouldCountFixedPostSlotMain,
        isSecurityPostAssignmentType,
        parseAssignedNames,
        pickPreferredRow,
        getStats
    };
})(window);
