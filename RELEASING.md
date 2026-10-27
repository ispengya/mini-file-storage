# 发布到 Maven Central

本项目通过 Sonatype Central Portal 发布,坐标为:

```text
io.github.ispengya:file-storage
```

## 1. 前置条件

- 构建环境:JDK 17 + Maven(产物目标是 Java 8 字节码)
- Central Portal 账号:<https://central.sonatype.com>
- 已验证的 namespace:`io.github.ispengya`
  - 用 GitHub 登录后通常可自动验证;若没有,按 Portal 显示的 verification key 创建一个同名公开仓库,再点 Verify。
- GPG 密钥(有私钥,公钥已上传到 keyserver)
- Central Portal user token(不是登录密码)

## 2. 配置 `~/.m2/settings.xml`

在 `<servers>` 中加入:

```xml
<server>
  <id>central</id>
  <username>TOKEN_USERNAME</username>
  <password>TOKEN_PASSWORD</password>
</server>
```

`id` 必须和 POM 里的 `publishingServerId` 一致(`central`)。

## 3. GPG

```bash
gpg --full-generate-key
gpg --list-secret-keys --keyid-format LONG
gpg --keyserver hkps://keys.openpgp.org --send-keys <KEY_ID>
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
```

当前发布密钥指纹:

```text
E0D0D22DBB41E745173E30AC356D8913E73CC2F8
```

passphrase 保存在仓库外的 `~/.m2/central-gpg-passphrase`,release profile 通过
`--passphrase-file` 读取它。不要把私钥、passphrase 或 Central token 提交到仓库。

## 4. 本地验证构建

```bash
mvn clean test
mvn -Prelease -Dgpg.skip=true -DskipTests package
```

第二条命令会额外生成 sources/javadoc jar,用于提前发现打包问题,但不签名。

## 5. 发布

```bash
mvn clean deploy -Prelease
```

首次发布使用 `autoPublish=false`:

1. 命令上传 bundle 并等待 Central Portal 校验(`waitUntil=validated`);
2. 打开 <https://central.sonatype.com/publishing>;
3. 确认 deployment 校验通过后点击 Publish。

发布成功后,依赖坐标:

```xml
<dependency>
  <groupId>io.github.ispengya</groupId>
  <artifactId>file-storage</artifactId>
  <version>0.1.0</version>
</dependency>
```

`repo1.maven.org` 同步可能需要几分钟,可以用下面的命令验证:

```bash
mvn dependency:get -Dartifact=io.github.ispengya:file-storage:0.1.0
```

## 6. 后续版本

- 修改 `pom.xml` 的 `<version>`(或用 `mvn versions:set -DnewVersion=x.y.z`);
- 提交并打 tag:

```bash
git tag v0.1.0
git push origin master --tags
```

- 再执行 `mvn clean deploy -Prelease`;
- 稳定后可以把 `autoPublish` 改为 `true`、`waitUntil` 改为 `published`,让上传通过校验后自动发布。

## 注意

- 版本号和 groupId 一旦发布不可修改。
- Central 要求主 jar、sources jar、javadoc jar 都有 `.asc` 签名。
- 发布前确保 `LICENSE`、POM 的 `licenses/developers/scm/url` 与实际仓库一致。
