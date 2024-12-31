    // Highlight active sidebar link based on scroll position
    const sections = document.querySelectorAll('.content > div');
    const navLinks = document.querySelectorAll('.sidebar a');

    window.addEventListener('scroll', () => {
        let current = '%$';

        sections.forEach(section => {
            const sectionTop = section.offsetTop - 70; // Offset for header
            if (scrollY >= sectionTop) {
                current = section.getAttribute('id');
            }
        });

        navLinks.forEach(link => {
            link.classList.remove('active');
            if (link.getAttribute('href').includes(current)) {
                link.classList.add('active');
            }
        });
    });

    // Smooth scrolling for sidebar links
    navLinks.forEach(link => {
        link.addEventListener('click', event => {
            event.preventDefault();
            const targetSection = document.querySelector(link.getAttribute('href'));
            targetSection.scrollIntoView({ behavior: 'smooth' });
        });
    });