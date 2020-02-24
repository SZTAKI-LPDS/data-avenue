import {DaService} from '../da.service';
import {MessageService} from '../message.service';
import {Component, OnInit} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'app-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.css']
})
export class AboutComponent implements OnInit {
  daVersion = '';
  daProtocols = '';

  constructor(public settingsModal: NgbActiveModal,
              public daService: DaService,
              private messageService: MessageService) {
  }

  ngOnInit() {
    this.getDAInfos();
  }

  getDAInfos() {
    this.daService.getVersion()
      .subscribe(ret => {
          this.daVersion = 'Version: ' + ret;

          this.daService.getProtocols()
            .subscribe(protocolList => {
                this.daProtocols = 'Supported protocols: ' + protocolList.join(', ');
              },
              error => {
                this.daProtocols = 'Can not get supported protocols. Error: ' + error;
              });
        },
        error => {
          this.daVersion = 'Can not connect to Data Avenue. Error: ' + error;
        });
  }

}
