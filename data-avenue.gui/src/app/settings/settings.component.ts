import {DaService} from '../da.service';
import {MessageService} from '../message.service';
import {Component, Input, OnInit} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html',
  styleUrls: ['./settings.component.css']
})
export class SettingsComponent implements OnInit {
  @Input() settingDaBaseUrl = '';
  @Input() settingDaAuthKey = '';
  @Input() settingExtendedMode = false;

  constructor(public settingsModal: NgbActiveModal,
              private daService: DaService,
              private messageService: MessageService) {
  }

  ngOnInit() {
    this.settingDaAuthKey = this.daService.daAuthKey;
    this.settingDaBaseUrl = this.daService.daBaseUrl;
    this.settingExtendedMode = this.daService.daAdvancedMode;
  }

  OnSubmit() {
    this.daService.daAuthKey = this.settingDaAuthKey;
    this.daService.daBaseUrl = this.settingDaBaseUrl;
    this.daService.daAdvancedMode = this.settingExtendedMode;
    this.messageService.add('Settings saved. Logs ' + (this.settingExtendedMode ? 'enabled' : 'disabled'), true);
    this.settingsModal.close();
  }
}
