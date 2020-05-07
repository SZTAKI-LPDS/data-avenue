import {ChangeDetectorRef, Component, Input, NgZone, OnInit, ViewChild} from '@angular/core';
import {NgbModal} from "@ng-bootstrap/ng-bootstrap";
import {DaService} from "../da.service";
import {MessageService} from "../message.service";
import {ModalAuthComponent} from "../modal-auth/modal-auth.component";
import {BrowserComponent} from "../browser/browser.component";
import {Item} from "../item";

@Component({
  selector: 'app-select',
  templateUrl: './select.component.html',
  styleUrls: ['./select.component.css', '../shared.css']
})
export class SelectComponent extends BrowserComponent{

  height = 300;
  selectMode = 'fd'; // 'f' = select file only; 'd' = select directory only; 'fd' = select file or directory
  authenticationReset = false;
  modalTitle = '';

  @ViewChild('modalselect', {static: false}) modalselect;

   constructor(public modalService: NgbModal,
               public daService: DaService,
               public messageService: MessageService,
               public changeDetector: ChangeDetectorRef,
               private ngZone: NgZone,) {
     super(modalService, daService, messageService, changeDetector);

   }

  ngOnInit() {
    this.initDA();
    // https://www.c-sharpcorner.com/blogs/call-angular-2-function-from-javascript
    window['angularComponentReference'] = { component: this, zone: this.ngZone,
      daSelect: (defaultUrl, selectMode, authenticationReset, callback) => this.angularFunctionCalled(defaultUrl, selectMode, authenticationReset, callback), };
    //(<HTMLInputElement>document.getElementById('proba')).value = 'Angularbol';
    this.selectedSide = this.LEFT;
  }

  angularFunctionCalled(pDefaultUrl, selectMode, authReset, callback) {
    console.log('Angular function is called - browse. url:' + pDefaultUrl + " select mode:" + selectMode + 'authenticationReset:' + authReset);
    this.currentdDir[this.LEFT] = pDefaultUrl;
    this.selectMode = selectMode;
    this.authenticationReset = authReset;
    console.log('authenticationReset:' + this.authenticationReset);
    if (this.selectMode == this.TYP_DIRECTORY){
      this.modalTitle = 'Select directory';
    } else if (this.selectMode == this.TYP_FILE){
      this.modalTitle = 'Select file';
    } else {
      this.modalTitle = 'Select file or directory';
    }
    if (this.daService.authList[this.LEFT].isAuthenticated && !this.authenticationReset){
      if (this.daService.authList[this.LEFT].url.startsWith(this.currentdDir[this.LEFT]) || this.currentdDir[this.LEFT].startsWith(this.daService.authList[this.LEFT].url)) {
        this.openSelect(callback);
      } else {
        this.openAuthSelect(callback);
      }
    } else {
      this.openAuthSelect(callback);
    }
  }

  openAuthSelect(callback) {
    const modalAuthRef = this.modalService.open(ModalAuthComponent);
    //this.isMainDisabled = true;
    modalAuthRef.componentInstance.currentdDir = this.currentdDir[this.LEFT];
    modalAuthRef.componentInstance.selectedSide = this.LEFT;
    modalAuthRef.result.then((result: string) => {
      console.log("Auth OK");
      this.openSelect(callback);
    }, (reason) => {
      //cancel
    });
  }

  openSelect(callback) {
    this.selectedItem = null;
    this.modalService.open(this.modalselect, { size: 'lg'}).result.then((result) => {
      if (result === this.BTN_OK) {
        console.log("Select OK");
        console.log("isAuthenticated:" + this.daService.authList[this.LEFT].isAuthenticated);
        console.log("xcredentials:" + this.daService.authList[this.LEFT].xcredentials);
        console.log("url:" + this.daService.authList[this.LEFT].url);
        console.log("type:" + this.daService.authList[this.LEFT].type);
        console.log("fields:" + this.daService.authList[this.LEFT].fields);
        console.log("displayName:" + this.daService.authList[this.LEFT].displayName);

        callback(this.currentdDir[this.LEFT]+this.selectedItem.name, this.daService.authList[this.LEFT].xcredentials);
      }
    }, (reason) => {
    });
    this.getDirectoryList(this.LEFT);
  }

 /*
 * Override
 * onSelect: select only one Item (click on item)
 */
  onSel(item: Item, event, activeI: number, side: number) {
    this.selectedItem = item;
    this.selectedSide = side;
    this.activeIndex[side] = activeI;
    if (item != null) {
      this.selectionAllFalse();
      item.sel = !item.sel;
    }
  }

  isSelectOKDisabled() {
    let returnValue = true;
    if (this.selectedItem){
      if (this.selectMode == this.TYP_DIRECTORY || this.selectMode == this.TYP_FILE) {
        const itemType = this.isDir(this.selectedItem.name) ? this.TYP_DIRECTORY : this.TYP_FILE;
        if (this.selectMode == itemType) {
          returnValue = false;
        }
      } else {
        returnValue = false;
      }
    }
    return returnValue;
  }
}
