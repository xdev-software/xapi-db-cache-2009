[![Latest version](https://img.shields.io/maven-central/v/com.xdev-software/xapi-db-cache-2009)](https://mvnrepository.com/artifact/com.xdev-software/xapi-db-cache-2009)
[![Build](https://img.shields.io/github/actions/workflow/status/xdev-software/xapi-db-cache-2009/checkBuild.yml?branch=develop)](https://github.com/xdev-software/xapi-db-cache-2009/actions/workflows/checkBuild.yml?query=branch%3Adevelop)
[![javadoc](https://javadoc.io/badge2/com.xdev-software/xapi-db-cache-2009/javadoc.svg)](https://javadoc.io/doc/com.xdev-software/xapi-db-cache-2009) 
# SqlEngine Database Adapter Caché 2009

The XDEV Application Framework provides an abstraction over database dialects as part of its SqlEngine. This module is the Database Adapter for [InterSystems Caché 2009](https://www.intersystems.com/products/cache/) which includes the Caché-specific implementation for database access.

### :information_source: Important Notes
*At the time of publishing this code, we couldn't find any compatible JDBC driver as a maven dependency for 'Cache 2009.1'.
If you want to use this adapter properly, you have to search for a working dependency/.jar by yourself. According to the [Caché documentation](https://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_INTRO) there should be a JDBC-driver jar shipped with the product - consider using it.*

## XDEV-IDE
The [XDEV(-IDE)](https://xdev.software/en/products/swing-builder) is a visual Java development environment for fast and easy application development (RAD - Rapid Application Development). XDEV differs from other Java IDEs such as Eclipse or NetBeans, focusing on programming through a far-reaching RAD concept. The IDE's main components are a Swing GUI builder, the XDEV Application Framework and numerous drag-and-drop tools and wizards with which the functions of the framework can be integrated.

The XDEV-IDE was license-free up to version 4 inclusive and is available for Windows, Linux and macOS. From version 5, the previously proprietary licensed additional modules are included in the IDE and the license of the entire product has been converted to a paid subscription model. The XDEV Application Framework, which represents the core of the RAD concept of XDEV and is part of every XDEV application, was released as open-source in 2008.

## Support
If you need support as soon as possible and you can't wait for any pull request, feel free to use [our support](https://xdev.software/en/services/support).

## Contributing
See the [contributing guide](./CONTRIBUTING.md) for detailed instructions on how to get started with our project.

## Dependencies and Licenses
View the [license of the current project](LICENSE) or the [summary including all dependencies](https://xdev-software.github.io/xapi-db-cache-2009/dependencies/)
