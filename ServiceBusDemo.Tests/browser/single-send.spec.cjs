const { test, expect } = require('@playwright/test');

for (const { label, input, error, invalid, valid, message } of [
    { label: 'Fail first N attempts', input: '#fail-first-count', error: 'fail-first-error',
      invalid: '1.5', valid: '2', message: 'Enter a whole number from 1 to 10.' },
    { label: 'Slow handler', input: '#delay-seconds', error: 'delay-seconds-error',
      invalid: '121', valid: '5', message: 'Enter a whole number from 1 to 120.' }
]) {
    test(`single send reports invalid ${label} in the browser`, async ({ page }) => {
        await page.goto('/');
        // Polling starts only after Blazor becomes interactive. The consumers need not be running.
        await expect(page.locator('#ConsumerOne .consumer-state')).not.toHaveText('Checking status…', { timeout: 20_000 });
        await page.getByLabel(label, { exact: true }).check();
        const field = page.locator(input);
        await expect(field).toBeVisible();
        await page.getByLabel('Message', { exact: true }).fill('Validate before publishing');
        await field.fill(invalid);
        await expect(field).toHaveAttribute('aria-invalid', 'false');

        // Click a real submit button: without novalidate the browser blocks Blazor's submit handler.
        await page.getByRole('button', { name: 'Send', exact: true }).click();
        await expect(field).toHaveAttribute('aria-invalid', 'true');
        await expect(field).toHaveAttribute('aria-describedby', error);
        await expect(page.locator(`#${error}`)).toHaveText(message);
        await expect(field).toBeFocused();

        await field.fill(valid);
        await expect(field).toHaveAttribute('aria-invalid', 'false');
        await expect(page.locator(`#${error}`)).toBeEmpty();
    });
}
