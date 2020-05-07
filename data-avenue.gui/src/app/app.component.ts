import {MessageService} from './message.service';
import {Component, ElementRef, OnInit} from '@angular/core';
import {DaService} from "./da.service";

@Component({
  selector: 'dataavenue-gui',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  title = 'Data Avenue GUI2';

  constructor(private messageService: MessageService,
              public daService: DaService,
              private eltRef:ElementRef) {
    let browserMode = eltRef.nativeElement.getAttribute(daService.C_BROWSER_MODE);
    console.log("dataavenue-gui config - browser mode: " + browserMode);
    if (browserMode == 'select') {
      daService.enablePanels = false;
    }
    let baseurl = eltRef.nativeElement.getAttribute(daService.C_BASE_URL);
    console.log("dataavenue-gui config - baseurl: " + baseurl);
    if (baseurl) {
      daService.daBaseUrl = baseurl;
    }
    let authkey = eltRef.nativeElement.getAttribute(daService.C_AUTH_KEY);
    console.log("dataavenue-gui config - authkey: " + authkey);
    if (authkey) {
      daService.daAuthKey = authkey;
    }
    let advancedmode = eltRef.nativeElement.getAttribute(daService.C_ADVANCED_MODE);
    console.log("dataavenue-gui config - advancedmode: " + advancedmode);
    if (advancedmode == 'true') {
      daService.daAdvancedMode = true;
    }
  }

  ngOnInit(): void {
    this.messageService.add('Welcome to Data Avenue GUI2!', true);
  }

  public log(txt: string) {
    console.log(txt);
  }
}
