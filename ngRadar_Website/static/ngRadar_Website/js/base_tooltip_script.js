function initializeTooltips(root = document) {
        root.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((element) => {
            bootstrap.Tooltip.getOrCreateInstance(element, {
                trigger: 'hover',
                delay: { show: 100, hide: 50 }
            });
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        initializeTooltips();
    });

    document.body.addEventListener("htmx:beforeSwap", function (event) {
        event.detail.target
            .querySelectorAll('[data-bs-toggle="tooltip"]')
            .forEach((element) => {
                const tooltip = bootstrap.Tooltip.getInstance(element);
                if (tooltip) {
                    tooltip.dispose();
                }
            });
    });

    document.body.addEventListener("htmx:afterSwap", function (event) {
        initializeTooltips(event.detail.target);
    });