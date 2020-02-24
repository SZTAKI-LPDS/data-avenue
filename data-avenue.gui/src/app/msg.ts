export class Msg {
  static readonly SUCCESS = 'success';
  static readonly INFO = 'info';
  static readonly WARNING = 'warning';
  static readonly DANGER = 'danger';
  typ: string;
  txt: string;
}
