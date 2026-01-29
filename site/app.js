document.addEventListener('DOMContentLoaded', () => {
    enableSmoothScroll();
    animateOnScroll();
    updateFooterYear();
});

function enableSmoothScroll() {
    const links = document.querySelectorAll('a[href^="#"]');

    links.forEach(link => {
        link.addEventListener('click', e => {
            const target = document.querySelector(link.getAttribute('href'));
            if (!target) return;

            e.preventDefault();
            target.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        });
    });
}

function animateOnScroll() {
    const elements = document.querySelectorAll(
        '.metric-value, .quality-fill, .card'
    );

    if (!('IntersectionObserver' in window)) return;

    const observer = new IntersectionObserver(entries => {
        entries.forEach(entry => {
            if (!entry.isIntersecting) return;

            const el = entry.target;
            el.style.opacity = '0';
            el.style.transform = 'translateY(20px)';

            requestAnimationFrame(() => {
                el.style.transition = 'opacity 0.4s ease, transform 0.4s ease';
                el.style.opacity = '1';
                el.style.transform = 'translateY(0)';
            });

            observer.unobserve(el);
        });
    }, { threshold: 0.1 });

    elements.forEach(el => observer.observe(el));
}

function formatCurrency(amount) {
    return new Intl.NumberFormat('en-AU', {
        style: 'currency',
        currency: 'AUD'
    }).format(amount);
}

function formatNumber(value) {
    return new Intl.NumberFormat('en-AU').format(value);
}

function updateFooterYear() {
    const footer = document.querySelector('.footer-bottom');
    if (!footer) return;

    const year = new Date().getFullYear();
    footer.innerHTML = footer.innerHTML.replace(/\d{4}/, year);
}

window.DataPlatformUtils = {
    formatCurrency,
    formatNumber
};
