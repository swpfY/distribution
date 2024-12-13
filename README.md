<p align="center">
<img style="align: center; padding-left: 10px; padding-right: 10px; padding-bottom: 10px;" width="238px" height="238px" src="./distribution-logo.svg" />
</p>


## Target

This project inherits from [Distribution](https://github.com/distribution/distribution) and add some new storage drivers. Additional support providers are as follows：

- [Tencent Cloud Object Stroage (COS)](https://www.tencentcloud.com/products/cos?lang=en)  ✅
- [AliCloud Object Storage Service (OSS)](https://www.alibabacloud.com/en/product/object-storage-service?_p_lc=1) ⌛️

## Provider

### Tencent Cloud Object Storage（COS）

| Parameter     | Required | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| secretid      | yes      | Your TencentCloud CAM SecretId.                              |
| secretkey     | yes      | Your TencentCloud CAM SecretKey.                             |
| region        | yes      | The COS region in which your bucket exists. For example `ap-chengdu`. |
| bucket        | yes      | The bucket name registered in cos. For example `test-registry-1101772061`. |
| rootdirectory | no       | This is a prefix that is applied to all S3 keys to allow you to segment data in your bucket if necessary. The default is empty. |
| serviceurl    | no       | It is used to get service. The default is `https://service.cos.myqcloud.com`. |

## Image

[dockerhub](https://hub.docker.com/explore) - [jideaflow/distribution](https://hub.docker.com/repository/docker/jideaflow/distribution/tags)

```bash
# the first version based on v3.0.0-rc.1, only support cos
docker pull jideaflow/distribution:v3.0.0-rc.1_cos
```



