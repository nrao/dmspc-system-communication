// =========================================================
// Generic DOM helpers
// =========================================================

function setText(
    id,
    value,
    fallback = "-"
) {
    const element =
        document.getElementById(id);

    if (!element) {
        return;
    }

    element.textContent = value ?? fallback;}


function setHidden(
    id,
    hidden
) {
    const element = document.getElementById(id);

    if (!element) {
        return;
    }

    element.classList.toggle(
        "d-none",
        hidden
    );
}


// =========================================================
// Global system status
// =========================================================

function updateSystemStatus(data) {
    const label =
        data.status_label
        ?? data.status_name
        ?? data.status
        ?? "Active";

    const message =
        data.message
        ?? "";

    setText(
        "system-status-label",
        label,
        ""
    );

    setText(
        "system-status-message",
        message,
        ""
    );
}


// =========================================================
// GBT panel
// =========================================================

function updateGbtPanel(event) {
    const data = event.detail;

    setText(
        "gbt-status",
        data.status_label
            ?? data.status_name
            ?? data.status
    );

    setText(
        "gbt-target",
        data.target
    );

    setText(
        "gbt-object-id",
        data.object_id
    );

    setText(
        "gbt-waveform",
        data.tx_waveform
    );

    setText(
        "gbt-recorded-waveform",
        data.rec_waveform
    );

    setText(
        "gbt-message",
        data.message
    );

    updateSystemStatus(
        data
    );

    updateWaveformRequester(
        data.waveform_requester
    );
}

function updateWaveformRequester(requester) {
    setText(
        "waveform-requester",
        requester,
        ""
    );

    const container =
        document.getElementById(
            "waveform-requester-container"
        );

    if (!container) {
        return;
    }

    container.classList.toggle(
        "d-none",
        !requester
    );
}


// =========================================================
// VLBA state
// =========================================================

function updateVlbaState(event) {
    const data = event.detail;
    updateSystemStatus(
        data
    );
}

// =========================================================
// VLBA expedat Progress Rows
// =========================================================

function createProgressRow(
    stationId,
    stationName
) {
    const list =
        document.getElementById(
            "vlba-progress-list"
        );

    if (!list) {
        return null;
    }

    const row =
        document.createElement(
            "div"
        );

    row.id =
        `progress-${stationId}`;

    row.className =
        "mb-3";

    row.innerHTML = `
        <div
            class="d-flex
                   justify-content-between"
        >
            <strong>
                VLBA-${stationName}
            </strong>

            <span
                class="progress-bytes"
            >
                0 B / 0 B
            </span>
        </div>

        <div
            class="progress"
            role="progressbar"
            aria-valuemin="0"
            aria-valuemax="100"
        >
            <div
                class="progress-bar"
                style="width: 0%"
                aria-valuenow="0"
            >
                0%
            </div>
        </div>
    `;

    list.appendChild(row);

    return row;
}

function updateProgressState(event) {
    const data = event.detail;

    const stationId =
        Number(data.station);

    const stationName =
        data.station_name;

    if (
        !stationId
        || !stationName
    ) {
        console.error(
            "[Home] Invalid progress payload:",
            data
        );

        return;
    }

    let row =
        document.getElementById(
            `progress-${stationId}`
        );

    if (!row) {
        row = createProgressRow(
            stationId,
            stationName
        );
    }

    if (!row) {
        return;
    }

    const percent =
        Math.min(
            100,
            Math.max(
                0,
                Number(
                    data.percent || 0
                )
            )
        );

    const bar =
        row.querySelector(
            ".progress-bar"
        );

    if (bar) {
        bar.style.width =
            `${percent}%`;

        bar.setAttribute(
            "aria-valuenow",
            percent
        );

        bar.textContent =
            `${percent.toFixed(1)}%`;
    }

    const bytes =
        row.querySelector(
            ".progress-bytes"
        );

    if (bytes) {
        bytes.textContent =
            `${formatBytes(
                data.received_bytes
            )} / ${formatBytes(
                data.total_bytes
            )}`;
    }
}

function formatBytes(bytes) {
    const value =
        Number(bytes || 0);

    if (value === 0) {
        return "0 B";
    }

    const units = [
        "B",
        "KB",
        "MB",
        "GB",
    ];

    const index = Math.min(
        Math.floor(
            Math.log(value)
            / Math.log(1024)
        ),
        units.length - 1
    );

    return (
        (
            value
            / Math.pow(
                1024,
                index
            )
        ).toFixed(1)
        + " "
        + units[index]
    );
}


// =========================================================
// DSOC + DDM Panel
// =========================================================

// build one blank row that looks right 
function createDdmRow(stationId) {
    const body = 
        document.getElementById(
            "dsoc-image-table-body"
        );
        
    if (!body) {
        return null;
    }

    const row =
        document.createElement(
            "tr"
        );

    row.id =
        `ddm-row-${stationId}`;

    row.innerHTML = `
        <td>
            Station ${stationId}
        </td>
        <td>
            <img
                class="ddm-thumbnail"
                alt="DDM thumbnail"
                style="max-width: 100px;"
            />
        </td>
        <td>
            <a
                class="ddm-link"
                target="_blank"
                rel = "noopener"
                >
                View Full image

            </a>
        </td>
    `;

    body.appendChild(row);

    return row;
}

// decide if update is relevant (has an image)
// find station if belong to, find that row and update it 
function updateDdmTable(event) {
    const data = event.detail;
    // for debugging:
    // console.log("[Home] DDM event:", data.image_key, data.rcvr_station);

    if (!data.event_uuid) {
        return;
    }

    // for debugging:
    const stationId = Number(data.rcvr_station);

    if (!stationId) {
        return;
    }

    let row =
        document.getElementById(
            `ddm-row-${stationId}`
        );

    // build a new row if not exist (when some of vlba stations have not submitted any image yet)
    if (!row) {
        row = createDdmRow(
            stationId
        );
    }

    if (!row) {
        return;
    }

    const imageUrl = 
        `/home/image/${data.event_uuid}/`;

    const img =
        row.querySelector(
            ".ddm-thumbnail"
        );

    if (img) {
        img.src = imageUrl;
    }
    
    const link =
        row.querySelector(
            ".ddm-link"
        );

    if (link) {
        link.href = imageUrl;
    }
}


// =========================================================
// DSOC panel
// =========================================================

function updateDsocPanel(event) {
    const data = event.detail;

    setText(
        "dsoc-message",
        data.message,
        ""
    );

    updateSystemStatus(
        data
    );
}


// =========================================================
// Waveform submission button
// =========================================================

function lockSubmitButton() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    button.disabled = true;
    button.textContent =
        "Generating ...";
}


function unlockSubmitButton() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    button.disabled = false;
    button.textContent =
        "Submit";
}


async function initializeSubmitLock() {
    const button =
        document.getElementById(
            "waveform-submit-btn"
        );

    if (!button) {
        return;
    }

    try {
        const response = await fetch(
            "/home/lock-status/"
        );

        if (!response.ok) {
            throw new Error(
                `HTTP ${response.status}`
            );
        }

        const data =
            await response.json();

        if (data.locked) {
            lockSubmitButton();
        } else {
            unlockSubmitButton();
        }

    } catch (error) {
        console.error(
            "[Home] Could not determine "
            + "initial submit lock:",
            error
        );

        button.disabled = true;
        button.textContent =
            "Lock unavailable";
    }
}


// =========================================================
// Waveform submission
// =========================================================

async function submitWaveform(event) {
    event.preventDefault();

    const waveformForm = event.currentTarget;

    try {
        const response = await fetch(
            waveformForm.action,
            {
                method: "POST",
                body: new FormData(
                    waveformForm
                ),
                credentials: "same-origin",
            }
        );

        const data =
            await response.json();

        if (response.status === 409) {
            lockSubmitButton();

            console.log(
                "[Home] "
                + data.message
            );

            return;
        }

        if (!response.ok) {
            console.error(
                "[Home] "
                + (
                    data.message
                    || "Waveform submission failed."
                )
            );

            return;
        }

        // Django server accepted the waveform and
        // created the submission lock.
        lockSubmitButton();

        console.log(
            "[Home] "
            + data.message
        );

    } catch (error) {
        console.error(
            "[Home] Could not submit waveform:",
            error
        );
    }
}


const waveformForm =
    document.getElementById(
        "waveform-form"
    );

if (waveformForm) {
    waveformForm.addEventListener(
        "submit",
        submitWaveform
    );
}

initializeSubmitLock();


// =========================================================
// Browser event listeners
// =========================================================

document.body.addEventListener(
    "gbtChanged",
    updateGbtPanel
);


document.body.addEventListener(
    "vlbaChanged",
    updateVlbaState
);


document.body.addEventListener(
    "dsocChanged",
    updateDsocPanel
);


document.body.addEventListener(
    "statusChanged",
    (event) => {
        updateSystemStatus(
            event.detail
        );
    }
);




// A waveform submission starts a new operation.
// GBT activity confirms that processing has begun.

document.body.addEventListener(
    "gbtChanged",
    () => {
        lockSubmitButton();
    }
);


// Unlock submit_waveform button once DSOC reports completion or failure.
document.body.addEventListener(
    "dsocChanged",
    (event) => {
        const data = event.detail;

        if (
            data.status_name === "COMPLETED" ||
            data.status_name === "FAILED"
        ) {
            unlockSubmitButton();
        }
    }
);

document.body.addEventListener(
    "progressChanged",
    updateProgressState
);

document.body.addEventListener(
    "imageChanged",
    updateDdmTable
);