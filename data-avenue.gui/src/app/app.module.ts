import {BrowserModule} from '@angular/platform-browser';
import {NgModule} from '@angular/core';
import {AppComponent} from './app.component';
import {AppRoutingComponent} from './app-routing.component';
import {MessagesComponent} from './messages/messages.component';
import {BrowserComponent} from './browser/browser.component';
import {DaService} from './da.service';
import {MessageService} from './message.service';
import {HttpClientModule} from '@angular/common/http';
import {FormsModule} from '@angular/forms'; // <-- NgModel lives here
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';
import {ModalAuthComponent} from './modal-auth/modal-auth.component';
import {FileUploadModule} from 'ng2-file-upload';
import {SettingsComponent} from './settings/settings.component';
import {AboutComponent} from './about/about.component';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {SimpleNotificationsModule} from 'angular2-notifications';


@NgModule({
  declarations: [
    AppComponent,
    AppRoutingComponent,
    MessagesComponent,
    BrowserComponent,
    ModalAuthComponent,
    SettingsComponent,
    AboutComponent
  ],
  entryComponents: [ModalAuthComponent,
    SettingsComponent,
    AboutComponent],
  imports: [
    BrowserModule,
    HttpClientModule,
    FormsModule,
    FileUploadModule,
    NgbModule,
    BrowserAnimationsModule,
    SimpleNotificationsModule.forRoot()
  ],
  providers: [DaService, MessageService],
  bootstrap: [AppComponent]
})
export class AppModule {
}
