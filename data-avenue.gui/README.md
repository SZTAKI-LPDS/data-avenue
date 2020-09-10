# DaGui2

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 1.6.5.

## Development server

Run `ng serve` for a dev server. Navigate to `http://localhost:4200/`. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng generate component component-name` to generate a new component. You can also use `ng generate directive|pipe|service|class|guard|interface|enum|module`.

## Build

Run `ng build` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `-prod` flag for a production build.

## Running unit tests

Run `ng test` to execute the unit tests via [Karma](https://karma-runner.github.io).

## Running end-to-end tests

Run `ng e2e` to execute the end-to-end tests via [Protractor](http://www.protractortest.org/).

## Further help

To get more help on the Angular CLI use `ng help` or go check out the [Angular CLI README](https://github.com/angular/angular-cli/blob/master/README.md).

# Usage

## Two panel mode

This is the default, two panel file manager mode.

## File chooser mode

In this mode, a file or directory selection function can be integrated into a web page. You can see an example usage in index.html.

- The Data Avenue js and css files are required. See index.html.
- Add the `<dataavenue-gui browsermode="select" baseurl="" authkey="" advancedmode="false"></dataavenue-gui>` html tag in the body section of your html page.
Set browsermode parameter value to select.
- Create two javascript functions `daSelect` and `callBackDaSelect`:
```
  function daSelect() {
    var defaultUrl = 'http://mirror.niif.hu/ubuntu/';
    var selectMode = 'f';
    var authenticationReset = false; 
    window.angularComponentReference.zone.run(() => {
      window.angularComponentReference.daSelect(defaultUrl, selectMode, authenticationReset, callBackDaSelect);
    });
  }

  function callBackDaSelect(returnedUrl, returnedCredential){

  }
```
If you want to activate the file chooser, call the `daSelect` function.
The function `window.angularComponentReference.daSelect` has four parameters:
   1. defaultUrl, file browsing starts from here e.g.: `http://mirror.niif.hu/ubuntu/`
   2. selectMode, 'f' = select file only; 'd' = select directory only; 'fd' = select file or directory
   3. authenticationReset, if true, then the second load of the same url requires the authentication again
   4. callBackDaSelect, is a callback function, it is called after the user finished the browse.
 
The callback function `callBackDaSelect` gets two parameters:
  1. returnedUrl, contains the url selected by the user
  2. returnedCredential, contains the credential selected by the user

These values can be used as desired.
