export class TransferStatus {
  static readonly CREATED = 'CREATED';
  static readonly TRANSFERRING = 'TRANSFERRING';
  static readonly COMPETED = 'COMPETED';
  static readonly FAILED = 'FAILED';
  static readonly CANCELED = 'CANCELED';
  static readonly DONE = 'DONE';
  id: string;
  bytesTransferred: number;
  source: string;
  status: string;
  serverTime: number;
  target: string;
  ended: number;
  started: number;
  size: number;
  percent: number;
  failure: string;
}
