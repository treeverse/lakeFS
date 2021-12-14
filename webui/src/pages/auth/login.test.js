
const webdriver = require("selenium-webdriver");

// async function testLoginWithValidData(){
//  try {
//
//     const driver = new webdriver.Builder().forBrowser('chrome').build();
//     await driver.get('http://localhost:3000/auth/login');
//
//     // const widget = await driver.findElement({css:"login-widget"});
//     const username = await driver.findElement({id:"username"});
//     const password = await driver.findElement({id:"password"});
//
//     username.sendKeys("AKIAIOSFODNN7EXAMPLE");
//     password.sendKeys("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
//     login.click();
//
//     driver.quit();
//  } catch (e) {
//      handleFailure(e, driver);
//  }
// }
//
// testLoginWithValidData();
//
// function handleFailure(err, driver) {
//     console.error('Something went wrong!\n', err.stack, '\n');
//     driver.quit();
// }

let driver;

beforeAll( async => {
    driver = new webdriver.Builder().forBrowser('chrome').build();
})

beforeEach(async () => {
    await driver.get('http://localhost:3000/auth/login',{waitUntil: 'domcontentloaded'});
});

afterAll(async () => {
    await driver.quit();
})

test('Header of the login widget', async () => {
    const cardHeader = await page.$('.card-header'); // example of selecting HTML elements by CSS selector
    const html = await page.evaluate(cardHeader => cardHeader.innerHTML, cardHeader);
    expect(html).toBe("Login");
}, 10000);

test('Submit login form with invalid data', async () => {
    await page.waitForSelector('.login-widget');
    await page.type('#username', 'INVALIDEXAMPLE');
    await page.type('#password','INVALIDEXAMPLE');

    await page.click('[type="submit"]');

    await page.waitForSelector('.login-error');
    const err = await page.$('.login-error'); // example of selecting HTML elements by CSS selector
    const html = await page.evaluate(err => err.innerHTML, err);
    expect(html).toBe("invalid credentials");
}, 10000);

test('Submit login form with valid data', async () => {
    await page.waitForSelector('.login-widget');
    await page.type('#username', 'AKIAIOSFODNN7EXAMPLE');
    await page.type('#password','wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY');

    await Promise.all([
        await page.click('[type="submit"]'),
        page.waitForNavigation(),
    ]);

    expect(page.url()).toBe("http://localhost:3000/repositories");
}, 10000);