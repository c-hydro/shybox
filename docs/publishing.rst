Publish on Read the Docs
========================

Read the Docs builds documentation directly from a Git repository. A local HTML
build is useful for review, but publishing requires the documentation sources
and ``.readthedocs.yaml`` to be available on GitHub.

First project setup
-------------------

#. Sign in at the `Read the Docs dashboard <https://app.readthedocs.org/dashboard/>`_.
#. Connect the GitHub account that can administer ``c-hydro/shybox``.
#. Choose **Add project** and select the SHYBOX repository. If requested, grant
   the Read the Docs GitHub App access to that repository.
#. Use ``.readthedocs.yaml`` as the configuration file.
#. Set the project's default branch to ``main``. The public ``latest`` version
   follows this branch; a successful build of a pull request is a preview,
   not confirmation that ``latest`` has updated.
#. Inspect the build log and open the resulting documentation URL.

If automatic import is unavailable, choose manual configuration with repository
URL ``https://github.com/c-hydro/shybox.git`` and configure the Git integration
after creating the project. The final project slug and public URL are assigned
in Read the Docs; this guide does not assume a particular slug is available.

Version management
------------------

Documentation edition **0.2.0** is published from ``main``. The initial edition
used a dedicated documentation branch; that is no longer the source of the
public ``latest`` version. Documentation edition numbers remain independent
of package version |package_version|.

Read the Docs versions correspond to Git branches or tags. In the project's
**Versions** tab, activate the documentation branch if you also want a separately
named version alongside ``latest``. The slash in a Git branch name may be
normalized in the Read the Docs version URL.

Merge reviewed documentation into ``main`` and check both the GitHub build and
the subsequent Read the Docs ``latest`` build.
For a future package release, ensure its Git tag includes the documentation and
configuration before activating the tag in Read the Docs. Do not expect older
code tags without these files to build the new documentation.

Build configuration
-------------------

The repository configuration selects Ubuntu 24.04, Python 3.12,
``docs/conf.py`` and the pinned dependencies in ``docs/requirements.txt``.
Sphinx warnings fail the remote build just as they do locally.

See the official `project import guide
<https://docs.readthedocs.com/platform/stable/intro/add-project.html>`_,
`configuration reference
<https://docs.readthedocs.com/platform/stable/config-file/v2.html>`_ and
`version management guide
<https://docs.readthedocs.com/platform/stable/versions.html>`_ for service details.
