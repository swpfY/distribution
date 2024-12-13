## Parameters

| Parameter     | Required | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| secretid      | yes      | Your TencentCloud CAM SecretId.                              |
| secretkey     | yes      | Your TencentCloud CAM SecretKey.                             |
| region        | yes      | The COS region in which your bucket exists. For example `ap-chengdu`. |
| bucket        | yes      | The bucket name registered in cos. For example `test-registry-1101772061`. |
| rootdirectory | no       | This is a prefix that is applied to all S3 keys to allow you to segment data in your bucket if necessary. The default is empty. |
| serviceurl    | no       | It is used to get service. The default is `https://service.cos.myqcloud.com`. |

## Reference

- [ Writing new registry storage drivers - distribution guide](https://distribution.github.io/distribution/storage-drivers/)

- [Cloud Object Storage（COS）- TencentCloud Doc](https://cloud.tencent.com/product/cos)

- [COS Go SDK - TencentCloud Doc](https://cloud.tencent.com/document/product/436/31215)