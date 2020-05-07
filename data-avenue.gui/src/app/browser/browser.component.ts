import {AboutComponent} from '../about/about.component';
import {ChangeDetectorRef, Component, ElementRef, HostListener, Input, OnInit, ViewChild, NgZone} from '@angular/core';
import {ModalDismissReasons, NgbModal, NgbTabChangeEvent} from '@ng-bootstrap/ng-bootstrap';
import {FileItem, FileUploader, ParsedResponseHeaders} from 'ng2-file-upload';
import {saveAs} from 'file-saver';

import {Item} from '../item';
import {Auth} from '../auth';
import {DaService} from '../da.service';
import {MessageService} from '../message.service';
import {ModalAuthComponent} from '../modal-auth/modal-auth.component';
import {Msg} from '../msg';
import {SettingsComponent} from '../settings/settings.component';
import {TransferStatus} from '../transferstatus';

@Component({
  selector: 'app-browser',
  templateUrl: './browser.component.html',
  styleUrls: ['./browser.component.css', '../shared.css']
})

export class BrowserComponent implements OnInit {
  public uploader: FileUploader = new FileUploader({
    url: this.daService.daBaseUrl + 'rest/file',
    disableMultipart: true,
    method: 'POST'
    //    autoUpload: true
  });
  public hasDropZoneOverLeft = false;
  public hasDropZoneOverRight = false;


  dirList: Item[][] = [new Array<Item>(), new Array<Item>()]; // [side][list]
  transferStatusList: TransferStatus[] = new Array<TransferStatus>();
  isLoad: boolean[] = [false, false]; // [side]
  acknowledging = 0; //active transfer status acknowledge operation
  isMainDisabled = false;
  @Input() isDetailed: boolean[] = [false, false]; // [side]
  isConnectedToDA = false;
  supportedProtocols: string[];
  @Input() currentdDir: string[] = ['', '']; // [side]
  activeIndex: number[] = [-1, -1]; // [side]
  selectedItem: Item = null;
  selectedSide = 2;
  mkdirName = ''; // make directory name
  @Input() authUser = '';
  @Input() authPass = '';

  height = 200;

  @ViewChild('downLoadLinkElement', {static: false}) downLoadLinkElement: ElementRef;
  @ViewChild('modalmkdir', {static: false}) modalmkdir;
  @ViewChild('modaldel', {static: false}) modaldel;


  LEFT = 0;
  RIGHT = 1;

  // Item typ
  TYP_DIRECTORY = 'd';
  TYP_FILE = 'f';

  // Button type
  BTN_OK = 1;
  BTN_CANCEL = 0;


  constructor(public modalService: NgbModal,
              public daService: DaService,
              public messageService: MessageService,
              public changeDetector: ChangeDetectorRef) {
    // upload
    this.uploader.onAfterAddingFile = (file) => {
      console.log(file);
      // x-uri must be the 0. value!
      file.headers = [
        {name: 'x-uri', value: this.currentdDir[this.selectedSide] + file._file.name},
        {name: 'x-key', value: this.daService.daAuthKey},
        {name: 'x-credentials', value: this.daService.authList[this.selectedSide].xcredentials}
      ];
      file.upload(); // upload immediately
    };

    this.uploader.onErrorItem = ((item: FileItem, response: string, status: number, headers: ParsedResponseHeaders): any => {
      this.logTyp('Upload error: ' + this.getNameFromUrl(item.headers[0].value) + ' ' + response, Msg.DANGER);
    });

    this.uploader.onSuccessItem = ((item: FileItem, response: string, status: number, headers: ParsedResponseHeaders): any => {
      this.logTyp('Upload Success: ' + this.getNameFromUrl(item.headers[0].value), Msg.SUCCESS);
      let itemDir = item.headers[0].value;
      itemDir = itemDir.substring(0, itemDir.lastIndexOf('/') + 1);
      console.log('onSuccessItem: itemDir=' + itemDir);
      if (this.currentdDir[this.LEFT] === itemDir) {
        this.getDirectoryListIfNotInProg(this.LEFT);
      } else if (this.currentdDir[this.RIGHT] === itemDir) {
        this.getDirectoryListIfNotInProg(this.RIGHT);
      }
    });
  }


  ngOnInit() {
    this.onResize();
    //this.onAbout();
    this.currentdDir[this.LEFT] = '';
    this.currentdDir[this.RIGHT] = '';
    this.selectedSide = this.LEFT;
    this.initDA();
    this.getTransfers();
  }

  /*
   * get version, load supported protocols
   */
  initDA() {
    this.daService.getVersion()
      .subscribe(ret => {
          this.isConnectedToDA = true;
          this.log('Connected to Data Avenue ' + this.daService.daBaseUrl + ' Version: ' + ret);

          this.daService.getProtocols()
            .subscribe(protocolList => {
                this.supportedProtocols = protocolList;
                this.log('Supported protocols: ' + this.supportedProtocols.toString());
              },
              error => {
                this.supportedProtocols = null;
                this.logTyp('Can not get supported protocols. Error: ' + error, Msg.DANGER);
              });
        },
        error => {
          this.isConnectedToDA = false;
          this.logTyp('Can not connect to Data Avenue. Error: ' + error, Msg.DANGER);
        });
  }

  @HostListener('window:resize', ['$event'])
  onResize(event?) {
    try {
      if (window.innerHeight > 300) {
        let neededWidth = 822;
        if (window.innerWidth > neededWidth) {
          this.height = window.innerHeight - 145;
        } else {
          this.height = window.innerHeight - 175;
        }
      } else {
        this.height = 150;
      }
      console.log('set panel Height:' + this.height + ' window.innerWidth' + window.innerWidth);
    } catch (e) {
      console.log(e);
    }
  }

  @HostListener('document:keydown', ['$event']) onKeydownHandler(event: KeyboardEvent) {
    if (!this.isMainDisabled) {
      switch (event.key) {
        case 'Escape':
          this.log('ESC pressed');
          break;
        case 'F1':
          this.log('F1 pressed');
          return false;
        case 'F2':
          this.log('F2 pressed');
          return false;
        case 'F3':
          this.log('F3 pressed');
          if (!this.isBtnDownloadDisabled()) {
            this.onDownload();
          }
          return false;
        case 'F4':
          this.log('F4 pressed');
          return false;
        case 'F5':
          this.log('F5 pressed');
          if (!this.isBtnCopyDisabled()) {
            this.onCopy(false);
          }
          return false;
        case 'F6':
          this.log('F6 pressed');
          if (!this.isBtnCopyDisabled()) {
            this.onCopy(true);
          }
          return false;
        case 'F7':
          this.log('F7 pressed');
          if (!this.isBtnMkDirDisabled()) {
            this.openMkDir(this.modalmkdir);
          }
          return false;
        case 'F8':
          this.log('F8 pressed');
          if (!this.isBtnDeleteDisabled()) {
            this.openDel();
          }
          return false;
        case 'F9':
          this.log('F9 pressed');
          return false;
        case 'F10':
          this.log('F10 pressed');
          return false;
        case 'Tab':
          this.log('Tab pressed');
          if (this.daService.authList[this.selectedSide].isAuthenticated) {
            this.selectedSide = this.getOppositeSide(this.selectedSide);
            if (!this.daService.authList[this.selectedSide].isAuthenticated) {
              this.onUseOther(this.selectedSide);
            }
            return false;
          }
          break;
        case '*':
          this.log('* pressed');
          this.selectionInvert();
          return false;
        default:
          // this.log('pressed:' + event.key);
          break;
      }
    } else {
      switch (event.key) {
        case 'F5':
          return false;
      }
    }
  }

  /*
   * in case of modal
   */
  public isEnterPressed($event, c) {
    if ($event.key == 'Enter') {
      this.log('ENTER pressed in modal');
      c(this.BTN_OK);
    }
  }

  public isEnterPressedOnUrl($event, side) {
    if ($event.key == 'Enter') {
      this.log('ENTER pressed on side:' + side);
      if (this.isConnectedToDA) {
        this.onUrlSelected(side);
      }
    }
  }

  /*
   * Tab menu
   */
  public beforeTabChange($event: NgbTabChangeEvent) {
    if ($event.nextId === 'tab-clrnotify') {
      this.messageService.clearNotifications();
      $event.preventDefault();
    } else if ($event.nextId === 'tab-settings') {
      this.onSettings();
      $event.preventDefault();
    } else if ($event.nextId === 'tab-about') {
      this.onAbout();
      $event.preventDefault();
    } else if ($event.nextId === 'tab-transfers') {
      this.isMainDisabled = true;
    } else if ($event.nextId === 'tab-uploads') {
      this.isMainDisabled = true;
    } else if ($event.nextId === 'tab-main') {
      this.isMainDisabled = false;
    }
  }

  /*
   * if the sides currentDir is root, disable change dir to prev
   */
  enablePrevDir(side: number): boolean {
    const n = this.currentdDir[side].split('/').length - 1;
    return n > 3;
  }

  /*
   * Change directory (dblclick on item)
   */
  onCD(item: Item, side: number) {
    if (this.isDir(item.name)) {
      this.selectedItem = null;
      this.changeDir(item.name, side);
    } else {// file
    }
  }

  /*
   * onSelect: select an Item (click on item)
   * select more items if ctrl or meta key pressed
   */
  onSel(item: Item, event, activeI: number, side: number) {
    this.selectedItem = item;
    this.selectedSide = side;
    this.activeIndex[side] = activeI;
    if (item != null) {
      if (!(event.ctrlKey || event.metaKey)) {
        this.selectionAllFalse();
      }
      item.sel = !item.sel;
    }
  }

  selectionInvert() {
    for (let i = 0; i < this.dirList[this.selectedSide].length; i++) {
      const item = this.dirList[this.selectedSide][i];
      if (this.activeIndex[this.selectedSide] < 0) {
        this.activeIndex[this.selectedSide] = i;
        this.selectedItem = item;
      }
      item.sel = !item.sel;
    }
  }

  selectionAllFalse() {
    for (let i = 0; i < this.dirList[this.selectedSide].length; i++) {
      this.dirList[this.selectedSide][i].sel = false;
    }
  }

  /*
   * get directory list (click on button GO)
   */
  onUrlSelected(side: number) {
    this.selectedSide = side;
    this.currentdDir[side] = this.currentdDir[side].trim();
    if (!this.currentdDir[side].endsWith('/')) {
      this.currentdDir[side] = this.currentdDir[side] + '/';
    }

    if (this.daService.authList[side].isAuthenticated) {
      if (this.daService.authList[side].url.startsWith(this.currentdDir[side]) || this.currentdDir[side].startsWith(this.daService.authList[side].url)) {
        this.getDirectoryList(this.selectedSide);
      } else {
        this.authenticate(side);
      }
    } else {
      this.authenticate(side);
    }
  }

  private authenticate(side: number) {
    const protocol = this.getProtocolFromUrl(this.currentdDir[side]);
    this.log('protocol: ' + protocol);
    if (protocol == null) {
      this.logTyp('Invalid url: ' + this.currentdDir[side], Msg.DANGER);
    } else {
      if (this.isProtocolSupported(protocol)) {
        this.openAuth();
      } else {
        this.logTyp('The protocol ' + protocol + ' is not supported!', Msg.DANGER);
      }
    }
  }

  /*
   * Use the other sides url and auth
   */
  onUseOther(thisSide: number) {
    const oppositSide = this.getOppositeSide(thisSide);
    this.daService.authList[thisSide] = {
      type: this.daService.authList[oppositSide].type,
      displayName: this.daService.authList[oppositSide].displayName,
      fields: this.daService.authList[oppositSide].fields,
      xcredentials: this.daService.authList[oppositSide].xcredentials,
      url: this.daService.authList[oppositSide].url,
      isAuthenticated: true
    };

    this.currentdDir[thisSide] = this.currentdDir[oppositSide];
    this.logTyp('Other side used.', Msg.INFO);
    this.onUrlSelected(thisSide);
  }

  /*
   * go to previous dir (dbclick on ..)
   */
  onChangeDirToPrev(side: number) {
    if (this.daService.authList[side].isAuthenticated) {
      this.currentdDir[side] = this.currentdDir[side].substring(0, this.currentdDir[side].lastIndexOf('/'));
      this.currentdDir[side] = this.currentdDir[side].substring(0, this.currentdDir[side].lastIndexOf('/')) + '/';
      this.getDirectoryList(side);
    } else {
      this.openAuth();
    }
  }

  /**
   * Settings selected
   */
  onSettings() {
    const modalSettingsRef = this.modalService.open(SettingsComponent);
    this.isMainDisabled = true;
    modalSettingsRef.result.then((result: string) => {
      this.uploader.options.url = this.daService.daBaseUrl + 'rest/file';
      this.initDA();
      this.isMainDisabled = false;
    }, (reason) => {
      this.log(`Settings Dismissed ${this.getDismissReason(reason)}`);
      this.isMainDisabled = false;
    });
  }

  /**
   * About selected
   */
  onAbout() {
    const modalSettingsRef = this.modalService.open(AboutComponent);
    this.isMainDisabled = true;
    modalSettingsRef.result.then((result: string) => {
      // this.log(`About  ${result}`);
      this.isMainDisabled = false;
    }, (reason) => {
      // this.log(`About Dismissed ${this.getDismissReason(reason)}`);
      this.isMainDisabled = false;
    });
  }

  /*
   * F5 pressed = copy, F6 = move selected
   */
  onCopy(move: boolean) {
    const oppositSide = this.getOppositeSide(this.selectedSide);
    this.isLoad[this.selectedSide] = true;

    for (let i = 0; i < this.dirList[this.selectedSide].length; i++) {
      const item = this.dirList[this.selectedSide][i];
      if (item.sel) {
        this.copy(this.currentdDir[this.selectedSide] + item.name,
          this.currentdDir[oppositSide],
          this.daService.authList[this.selectedSide],
          this.daService.authList[oppositSide],
          move);
        item.sel = false;
      }

      this.isLoad[this.selectedSide] = false;
    }
  }

  /*
   * F7 pressed = make dir (after openMkDir OK)
   */
  onMkDir() {
    if (this.mkdirName.length === 0) {
      this.logTyp('Add new directory name!', Msg.WARNING);
    } else {
      if (!this.mkdirName.endsWith('/')) {
        this.mkdirName = this.mkdirName + '/';
      }
      this.makeDirectory(this.currentdDir[this.selectedSide] + this.mkdirName);
      this.mkdirName = '';
    }
  }

  /*
   * F8 pressed = delete selected
   */
  onDelete() {
    const delList: string[] = new Array<string>();
    for (let i = 0; i < this.dirList[this.selectedSide].length; i++) {
      const item = this.dirList[this.selectedSide][i];
      if (item.sel) {
        delList.push(item.name);
        item.sel = false;
      }
    }

    for (let i = 0; i < delList.length; i++) {
      const delItemName = delList[i];
      let reloadDir = false;
      if (i + 1 === delList.length) {
        reloadDir = true;
      }
      if (this.isDir(delItemName)) {
        this.deleteDirectory(this.currentdDir[this.selectedSide] + delItemName, reloadDir);
      } else {
        this.deleteFile(this.currentdDir[this.selectedSide] + delItemName, reloadDir);
      }
    }
  }

  /*
   * Download button pressed (old version)
   */
  onDownload1() {
    const downloadFileUrl = this.currentdDir[this.selectedSide] + this.selectedItem.name;

    this.log('Downloading file: ' + downloadFileUrl);
    this.daService.download(downloadFileUrl, this.daService.authList[this.selectedSide])
      .subscribe(data => {
          this.logTyp('File downloaded, save it: ' + downloadFileUrl, Msg.SUCCESS);
          saveAs(data, this.selectedItem.name);
        },
        error => {
          this.logTyp('download error: ' + error, Msg.DANGER);
        });
  }

  /*
  * Download button pressed (download the last selected)
  */
  onDownload() {
    const downloadFileUrl = this.currentdDir[this.selectedSide] + this.selectedItem.name;

    this.log('Downloading file: ' + downloadFileUrl);
    this.daService.downloadUrl(downloadFileUrl, this.daService.authList[this.selectedSide])
      .subscribe(data => {
          const fileUrl = this.daService.getdownloadUrl(data.toString());
          this.logTyp('Downloading file ' + this.selectedItem.name, Msg.SUCCESS);
          const el: HTMLElement = this.downLoadLinkElement.nativeElement as HTMLAnchorElement;
          el.setAttribute('href', fileUrl);
          el.click();
        },
        error => {
          this.logTyp('Download error: ' + error, Msg.DANGER);
        });
  }

  /**
   * Abort button at the transfer pressed
   */
  doAbortTransfer(transferId: string) {
    this.daService.copyAbort(transferId)
      .subscribe(ret => {
          this.log('Data transfer abort called: ' + ret);
        },
        error => {
          this.logTyp('Data transfer abort error: ' + error, Msg.DANGER);
        });
  }

  changeDir(urlModifier: string, side: number) {
    this.currentdDir[side] = this.currentdDir[side] + urlModifier;
    this.getDirectoryList(side);
  }

  /* modal */
  openDel() {
    this.isMainDisabled = true;
    this.modalService.open(this.modaldel).result.then((result) => {
      if (result === this.BTN_OK) {
        this.onDelete();
      }
      // this.log(`Closed with: ${result}`);
      this.isMainDisabled = false;
    }, (reason) => {
      // this.log(`Dismissed ${this.getDismissReason(reason)}`);
      this.isMainDisabled = false;
    });
  }

  openMkDir(content) {
    this.isMainDisabled = true;
    this.modalService.open(content).result.then((result) => {
      if (result === this.BTN_OK) {
        this.onMkDir();
      }
      this.isMainDisabled = false;
    }, (reason) => {
      this.isMainDisabled = false;
    });
  }

  openAuth() {
    const modalAuthRef = this.modalService.open(ModalAuthComponent);
    this.isMainDisabled = true;
    modalAuthRef.componentInstance.currentdDir = this.currentdDir[this.selectedSide];
    modalAuthRef.componentInstance.selectedSide = this.selectedSide;
    modalAuthRef.result.then((result: string) => {
      this.isMainDisabled = false;
      this.getDirectoryList(this.selectedSide);
    }, (reason) => {
      this.isMainDisabled = false;
    });
  }

  private getDismissReason(reason: any): string {
    if (reason === ModalDismissReasons.ESC) {
      return 'by pressing ESC';
    } else if (reason === ModalDismissReasons.BACKDROP_CLICK) {
      return 'by clicking on a backdrop';
    } else {
      return `with: ${reason}`;
    }
  }

  /* upload */

  public fileOverLeft(e: any): void {
    this.hasDropZoneOverLeft = e;
    this.selectedSide = this.LEFT;
  }

  public fileOverRight(e: any): void {
    this.hasDropZoneOverRight = e;
    this.selectedSide = this.RIGHT;
  }

  public setMethod(item: FileItem, overwrite: boolean) {
    console.log('setMethod. overwrite:' + overwrite + ' filename:' + item.file.name);
    if (overwrite) {
      item.method = 'PUT';
    } else {
      item.method = 'POST';
    }
  }

  /* DA service calls */


  extractData(res: string) {
    const myBlob: Blob = new Blob([res], {type: 'application/file'});
    const fileURL = URL.createObjectURL(myBlob);
    window.open(fileURL);
  }

  deleteFile(delUrl: string, reloadDir: boolean) {
    this.daService.deleteFile(delUrl, this.daService.authList[this.selectedSide])
      .subscribe(ret => {
        if (reloadDir) {
          this.getDirectoryList(this.selectedSide);
        }
      });
  }

  deleteDirectory(delUrl: string, reloadDir: boolean) {
    this.daService.deleteDirectory(delUrl, this.daService.authList[this.selectedSide])
      .subscribe(ret => {
        if (reloadDir) {
          this.getDirectoryList(this.selectedSide);
        }
      });
  }

  makeDirectory(Url: string) {
    this.daService.makeDirectory(Url, this.daService.authList[this.selectedSide])
      .subscribe(ret => {
        this.getDirectoryList(this.selectedSide);
      });
  }

  copy(fromUrl: string, toUrl: string, fromAuth: Auth, toAuth: Auth, move: boolean): void {
    this.daService.copy(fromUrl, toUrl, fromAuth, toAuth, move)
      .subscribe(ret => {
          this.log('Data transfer started. Data transfer id: ' + ret);
          this.updateTransferStatus('' + ret, move, 1000);
        },
        error => {
          this.logTyp('Data transfer error: ' + error, Msg.DANGER);
        });
  }

  updateTransferStatus(transferId: string, move: boolean, timeout: number) {
    this.daService.copyStatus(transferId).subscribe(ts => {
        ts.id = transferId;
        if (ts.bytesTransferred === 0 || ts.size === 0) {
          ts.percent = 0;
        } else {
          ts.percent = Math.floor((ts.bytesTransferred / ts.size) * 100);
        }

        if (ts.status === TransferStatus.DONE) {
          ts.percent = 100;
          this.logTyp(`${this.getNameFromUrl(ts.source)} transfer status: ${ts.status} `, Msg.SUCCESS);
        } else if (ts.status === TransferStatus.FAILED) {
          this.logTyp(`${this.getNameFromUrl(ts.source)} transfer status: ${ts.status} ${ts.failure}`, Msg.DANGER);
        } else {
          this.logTyp(`${this.getNameFromUrl(ts.source)} transfer status: ${ts.status} ${ts.percent}% `, Msg.INFO);
        }

        this.transferStatusListUpdate(ts);

        if ((ts.status === TransferStatus.COMPETED) ||
          (ts.status === TransferStatus.DONE) ||
          (ts.status === TransferStatus.FAILED) ||
          (ts.status === TransferStatus.CANCELED)) {

          const targetDirUrl = this.getDirFromUrl(ts.target);
          if (this.currentdDir[this.RIGHT] === targetDirUrl) {
            this.getDirectoryListIfNotInProg(this.RIGHT);
          } else if (this.currentdDir[this.LEFT] === targetDirUrl) {
            this.getDirectoryListIfNotInProg(this.LEFT);
          }
          // reload source
          if (move) {
            const sourceDirUrl = this.getDirFromUrl(ts.source);
            if (this.currentdDir[this.RIGHT] === sourceDirUrl) {
              this.getDirectoryListIfNotInProg(this.RIGHT);
            } else if (this.currentdDir[this.LEFT] === sourceDirUrl) {
              this.getDirectoryListIfNotInProg(this.LEFT);
            }
          }
        } else {
          setTimeout(() => {
            if (timeout < 5000) {
              timeout += 1000;
            }
            this.updateTransferStatus(transferId, move, timeout);
          }, timeout);
        }
      },
      terror => {
        this.logTyp('Data transfer status error: ' + terror, Msg.DANGER);
      }
    );
  }

  transferStatusListUpdate(status: TransferStatus) {
    let updated = false;
    for (let i = 0; i < this.transferStatusList.length; i++) {
      if (this.transferStatusList[i].id === status.id) {
        this.transferStatusList[i] = status;
        updated = true;
      }
    }
    if (!updated) {
      this.transferStatusList.push(status);
    }
  }

  transferStatusListRemove(transferId: string) {
    this.acknowledging++;
    this.daService.copyAcknowledge(transferId)
      .subscribe(ret => {
          this.log('Data transfer acknowledge called: ' + transferId);
          this.transferStatusList.forEach((transfer, index) => {
            if (transfer.id === transferId) {
              this.transferStatusList.splice(index, 1);
            }
          });
          this.acknowledging--;
        },
        error => {
          this.logTyp('Data transfer acknowledge error: ' + error, Msg.DANGER);
          this.acknowledging--;
        });
  }

  acknowledgeAllTransfer() {
    for (let transfer of this.transferStatusList) {
      if (transfer.status != 'TRANSFERRING') {
        this.transferStatusListRemove(transfer.id);
      }
    }
  }

  getTransfers() {
    this.daService.getTransfers()
      .subscribe(transfersList => {
          this.transferStatusList = transfersList;
        },
        error => {
          this.logTyp('Get transfer statuses error: ' + error, Msg.DANGER);
        });
  }

  getDirectoryListIfNotInProg(side: number): void {
    if (this.isLoad[side]) {
      this.log('Get directory in progress, skip this request on side ' + side);
    } else {
      this.getDirectoryList(side);
    }
  }

  getDirectoryList(side: number): void {
    this.isLoad[side] = true;
    this.activeIndex[side] = -1;

    if (this.isDetailed[side]) {
      this.daService.getDirectoryDetailedList(this.currentdDir[side], this.daService.authList[side])
        .subscribe(dirList => {
            this.isLoad[side] = false;

            const dirList2: Item[] = [];
            let index = 0;
            if (dirList.length > 0 && dirList[0].name.endsWith('../')) {
              index = 1; // in case of http ../ is in the list
            }
            for (; index < dirList.length; index++) {
              let item = dirList[index];
              item.size = this.formatSize(item.size);
              dirList2.push(item);
            }

            this.dirList[side] = dirList2;
          },
          error => {
            this.isLoad[side] = false;
            this.logTyp('Get detailed directory error: ' + error, Msg.DANGER);
            this.logTyp('Authentication RESET', Msg.WARNING);
            this.daService.authList[side].isAuthenticated = false;
            const dirList: Item[] = [];
            this.dirList[side] = dirList;
          });
    } else {
      this.daService.getDirectoryList(this.currentdDir[side], this.daService.authList[side])
        .subscribe(dirList => {
            this.isLoad[side] = false;
            const dirList2: Item[] = [];
            let index = 0;
            if (dirList.length > 0 && dirList[0].endsWith('../')) {
              index = 1; // in case of http ../ is in the list
            }
            for (; index < dirList.length; index++) {
              const name: string = dirList[index];
              dirList2.push({name: name, date: 0, size: null, sel: false});
            }
            this.dirList[side] = dirList2;
          },
          error => {
            this.isLoad[side] = false;
            this.logTyp('Get directory error: ' + error, Msg.DANGER);
            this.logTyp('Authentication RESET', Msg.WARNING);
            this.daService.authList[side].isAuthenticated = false;
            const dirList: Item[] = [];
            this.dirList[side] = dirList;
          });
    }
  }

  // disabled functions

  isBtnCopyDisabled(): boolean {
    if (!this.daService.authList[this.selectedSide].isAuthenticated ||
      !this.daService.authList[this.getOppositeSide(this.selectedSide)].isAuthenticated ||
      this.selectedItem == null ||
      this.currentdDir[this.LEFT] === this.currentdDir[this.RIGHT]) {
      return true;
    }
    return false;
  }

  isBtnMkDirDisabled(): boolean {
    return !this.daService.authList[this.selectedSide].isAuthenticated;
  }

  isBtnDeleteDisabled(): boolean {
    return !this.daService.authList[this.selectedSide].isAuthenticated || this.selectedItem == null;
  }

  isBtnDownloadDisabled(): boolean {
    return !this.daService.authList[this.selectedSide].isAuthenticated || this.selectedItem == null || this.isDir(this.selectedItem.name);
  }

  isBtnUploadDisabled(): boolean {
    return !this.daService.authList[this.selectedSide].isAuthenticated;
  }

  isDir(fileName: string): boolean {
    if (fileName.endsWith('/')) {
      return true;
    }
    return false;
  }

  getType(fileName: string): string {
    if (fileName.endsWith('/')) {
      return this.TYP_DIRECTORY;
    }
    return this.TYP_FILE;
  }

  getNameFromUrl(url: string): string {
    if (this.getType(url) === this.TYP_FILE) {
      return url.substring(url.lastIndexOf('/') + 1, url.length);
    } else {
      url = url.substring(0, url.lastIndexOf('/'));
      return url.substring(url.lastIndexOf('/') + 1, url.length) + '/';
    }
  }

  getDirFromUrl(url: string): string {
    if (url.endsWith('/')) {
      return url;
    } else {
      return url.substring(0, url.lastIndexOf('/') + 1);
    }
  }

  getProtocolFromUrl(url: string): string {
    const ddot = url.indexOf(':');
    if (ddot < 0) {
      return null;
    } else {
      return url.substring(0, ddot);
    }
  }

  isProtocolSupported(protocol: string): boolean {
    for (let index = 0; index < this.supportedProtocols.length; index++) {
      if (this.supportedProtocols[index] === protocol) {
        return true;
      }
    }
    return false;
  }

  getOppositeSide(thisSide: number): number {
    if (thisSide === this.LEFT) {
      return this.RIGHT;
    }
    return this.LEFT;
  }

  formatSize(pSize: string): string {
    if (pSize != null) {
      let size = Number(pSize);
      if (size < 1024) {
        return size + ' B';
      } else if (size < 1048576) {
        size = size / 1024;
        return size.toLocaleString(undefined, {maximumFractionDigits: 2, minimumFractionDigits: 2}) + ' kB';
      } else if (size < 1073741824) {
        size = size / 1048576;
        return size.toLocaleString(undefined, {maximumFractionDigits: 2, minimumFractionDigits: 2}) + ' MB';
      } else {
        size = size / 1073741824;
        return size.toLocaleString(undefined, {maximumFractionDigits: 2, minimumFractionDigits: 2}) + ' GB';
      }
    }
    return '-';
  }

  private log(message: string) {
    this.messageService.add(message, this.daService.daAdvancedMode);
  }

  private logTyp(message: string, typ: string) {
    this.messageService.addTyp(message, typ, this.daService.daAdvancedMode);
  }
}
