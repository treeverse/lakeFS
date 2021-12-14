const webdriver = require("selenium-webdriver");
const {until} = require("selenium-webdriver");
const LakefsUrl = "http://localhost:3000"

describe('executing tests on login page with Selenium', () => {

    let driver;
    driver = new webdriver.Builder().forBrowser('chrome').build();

    beforeEach(async () => {
        await driver.get(LakefsUrl+'/auth/login',{waitUntil: 'domcontentloaded'});
    })

    afterAll(async () => {
        await driver.quit();
    })

    test('Submit login form with invalid data', async () => {
        const login = await driver.findElement({className:"login-btn"});
        const username = await driver.findElement({id:"username"});
        await driver.executeScript("arguments[0].setAttribute('value', 'INVALIDEXAMPLE')", username);
        const password = await driver.findElement({id:"password"});
        await driver.executeScript("arguments[0].setAttribute('value', 'INVALIDEXAMPLE')", password);

        await login.click();
        const until = webdriver.until;
        var err = driver.wait(until.elementLocated({className:'login-error'}), 5000);
        const errTxt = await err.getAttribute("innerHTML");
        expect(errTxt).toBe("invalid credentials");
    }, 10000);

    test('Submit login form with valid data', async () => {
        const login = await driver.findElement({className:"login-btn"});
        const username = await driver.findElement({id:"username"});
        await driver.executeScript("arguments[0].setAttribute('value', 'AKIAIOSFODNN7EXAMPLE')", username);
        const password = await driver.findElement({id:"password"});
        await driver.executeScript("arguments[0].setAttribute('value', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')", password);

        await Promise.all([
            await login.click(),
            driver.wait(until.urlIs("http://localhost:3000/repositories"), 5000)
        ]);
        const actual = await driver.getCurrentUrl();
        expect(actual).toBe("http://localhost:3000/repositories");
    }, 10000);
})
