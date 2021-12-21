# Proposal: How to test the lakeFS UI

Currently, there are no automated tests for the lakeFS UI. The lack of automated tests decreases the confidence to 
introduce UI changes, reduces development velocity, and leads to unnoticed bugs of a changing severity, that
can sometimes even effect main UI workflows. 

Given that we currently have zero UI testing and with cost-effectivity in mind, this proposal suggests an incremental 
approach that will take us from zero to confidence in the main UI workflows, and does not aim to introduce a 100% 
coverage solution.

### Goals

* Increase the confidence that main UI workflows do not break.
* Minimize the cost of writing tests.
* Define a testing scope that is achievable at a reasonable timeframe.
* The tests are used both locally and as part of CI.
* Use JavaScript to test code written in JavaScript.

### Non-goals 

* Unit-test the [webui package](../webui)
* Test the UI visualization
* Stress-test the UI

## Proposal 

Implement E2E testing for a set of main UI workflows against a real lakeFS server. Use [Jest](https://jestjs.io/docs/tutorial-react)
which is the [most recommended](https://reactjs.org/docs/testing.html#tools) testing framework for React apps together 
with a testing automation framework of our choice ([Puppeteer](https://github.com/puppeteer/puppeteer) or 
[Selenium](https://www.selenium.dev/documentation/)) that allow interacting with a browser for testing purposes.

Below is a comparison of the two testing automation frameworks based on research and experiment followed by code examples. 

### Puppeteer Vs. Selenium 

|  | **Puppeteer** | **Selenium** | 
| :---:       | :---:         | :---:              |
|    **Development experience**        |     Good experience, it was easy to understand what to search for, and I found the code readable.         |        Unpleasant experience. due to the lack of documentation, and some basic functionality that's not working for the JavaScript-Selenium combination. I had to find [workarounds](https://stackoverflow.com/questions/25583641/set-value-of-input-instead-of-sendkeys-selenium-webdriver-nodejs) to trigger simple operations. Also, it took double the time to write the same tests with Selenium and it was the second framework I experimented with (I gained some experience working with Puppeteer)|
|   **Available docs (Jest + #)**         |        It is easy to find online resources because Puppeteer and Jest are Node libraries      |       It was hard to find references for Selenium in JavaScript, most docs include Java or Python examples.|
|   **Debugging options**         |     Can slow down the test and view a browser running the test, can use a debugger          |        Can view a browser running the test and use a debugger            |
|       **Setup experience**     |    Easy to install and configure           |         Easy to install, does not require additional configurations           |
|      **Supported browsers**      |   Chrome           |         Supports number of browsers          |

### Example Code 

The code below demonstrates how to use each testing automation framework to implement tests for the lakeFS Login workflow. 
The tests assume a running local lakeFS server.

#### Jest & Puppeteer 

```javascript
//login.test.js
const timeout = process.env.SLOWMO ? 30000 : 10000;

describe('executing tests on login page with Puppeteer', () => {
    beforeEach(async () => {
        await page.goto(`${URL}/auth/login`, {waitUntil: 'domcontentloaded'});
    });

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
})
```

#### Jest & Selenium

```javascript
//login.test.js
const webdriver = require("selenium-webdriver");
const {until} = require("selenium-webdriver");
const LakefsUrl = "http://localhost:8000"

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
        await driver.executeScript("arguments[0].setAttribute('value', 'INVALIDEXAMPLE')", username); // https://stackoverflow.com/questions/25583641/set-value-of-input-instead-of-sendkeys-selenium-webdriver-nodejs
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
            driver.wait(until.urlIs("http://localhost:3000/repositories"), 5000) // I didn't find a stright forward way to wait for navigation
        ]);
        const actual = await driver.getCurrentUrl();
        expect(actual).toBe("http://localhost:3000/repositories");
    }, 10000);
})
```

### Recommendation 

The main arguments that turn my recommendation towards **Puppeteer** as the testing automation framework are:
* The lakeFS UI is written in React, and using a framework that lives at the same space is more natural and convenient, 
as opposed to Selenium which is used for multiple types of applications. 
* The development experience working with Puppeteer was much better.
* Online resources testify that Puppeteer executes faster than Selenium.  

### Why test against a live server?

Writing and maintaining tests that rely on a live server is faster than using mocks. 
Also, by using a live server changes to the server code are automatically tested from the UI side. 

### How to make the tests runnable locally and part of the CI process

To allow running the tests locally and as part of CI, we need to spin up a lakeFS and postgres instances
the tests can communicate with. We can use [Testcontainers](https://www.npmjs.com/package/testcontainers) to 
spin the instances up as docker containers. 

Then, to run the tests locally we can use
```shell
npm test
```

As for CI, the [node workflow](../.github/workflows/node.yaml) is already running the webui tests that currently does 
not exist. after imlementing the tests this workflow will be responsible for running them.   

_**Notes:**_
1. The decision on the testing automation framework will not affect this section. 
2. Its TBD to decide exactly how to work with Testcontainers to do the tests setup; what the containers 
should include, and the frequency of creating and tearing them down (In each test file, single instance for the whole 
test suite etc.)

### Future improvements

In the proposed increment, Jest is used as the E2E tests runner. As a second step we can use Jest in combination with
other React testing libraries to to unit-test the UI. As a third increment, when the UI becomes more stable we can use more advanced features provided by the testing automation framework such as screenshot
testing, performance testing, etc.

### Decisions

* Use Jest and Puppeteer.
* Use [Testcontainers](https://www.npmjs.com/package/testcontainers) to spin up containerized lakeFS test instances.

### References
* https://medium.com/touch4it/end-to-end-testing-with-puppeteer-and-jest-ec8198145321
* https://www.browserstack.com/guide/puppeteer-vs-selenium
* https://www.browserstack.com/guide/automation-using-selenium-javascript
* https://jestjs.io/docs/puppeteer
* https://github.com/puppeteer/puppeteer
* https://levelup.gitconnected.com/running-puppeteer-with-jest-on-github-actions-for-automated-testing-with-coverage-6cd15bc843b0
* https://blog.testproject.io/2020/02/20/selenium-vs-puppeteer-when-to-choose-what/
* https://www.blazemeter.com/blog/selenium-vs-puppeteer-for-test-automation
