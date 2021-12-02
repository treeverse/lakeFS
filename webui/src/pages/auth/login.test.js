
const timeout = process.env.SLOWMO ? 30000 : 10000;

beforeEach(async () => {
    await page.goto(`${URL}/auth/login`, {waitUntil: 'domcontentloaded'});
});

test('Header of the login widget', async () => {
    const cardHeader = await page.$('.card-header'); // example of selecting HTML elements by CSS selector
    const html = await page.evaluate(cardHeader => cardHeader.innerHTML, cardHeader);
    expect(html).toBe("Login");
}, timeout);

test('Submit login form with invalid data', async () => {
    await page.waitForSelector('.login-widget');
    await page.type('#username', 'INVALIDEXAMPLE');
    await page.type('#password','INVALIDEXAMPLE');

    await page.click('[type="submit"]');

    await page.waitForSelector('.login-error');
    const err = await page.$('.login-error'); // example of selecting HTML elements by CSS selector
    const html = await page.evaluate(err => err.innerHTML, err);
    expect(html).toBe("invalid credentials");
}, timeout);

test('Submit login form with valid data', async () => {
    await page.waitForSelector('.login-widget');
    await page.type('#username', 'AKIAIOSFODNN7EXAMPLE');
    await page.type('#password','wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');

    await Promise.all([
        await page.click('[type="submit"]'),
        page.waitForNavigation(),
    ]);

    expect(page.url()).toBe("http://localhost:3000/repositories");
}, timeout);
