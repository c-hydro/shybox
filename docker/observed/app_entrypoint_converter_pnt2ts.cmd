## --------------------------------------------------------------------------------------------------
## FREE SPACE
# docker not used volume, containers ...
docker image prune -a

# singularity/apptainer tmp
apptainer cache clean blob
## --------------------------------------------------------------------------------------------------

## --------------------------------------------------------------------------------------------------
## BUILD DOCKERS
# clone git branch destine
git clone --branch destine https://github.com/c-hydro/shybox.git package_shybox

# docker build shybox
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-builder-shybox -t shybox-builder:dev .

# docker build converter [NO-CACHE]
DOCKER_BUILDKIT=1 docker build --no-cache --progress=plain -f Dockerfile --target app-converter-pnt2ts -t converter_pnt2ts:dev .

# docker build converter [ONLY CONVERTER]
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-converter-pnt2ts -t converter_pnt2ts:dev .

DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile --target app-converter-pnt2ts -t converter_pnt2ts:dev .
## --------------------------------------------------------------------------------------------------

