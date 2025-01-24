FROM docker.io/node:20.18.1-alpine3.21

ENV NO_UPDATE_NOTIFIER=true
WORKDIR /opt/app-root/src/app
COPY . /opt/app-root/src
RUN npm ci 
EXPOSE 9998
# Create the /.npm directory and grant access to group 0 to allow npm v9 to work
# See: https://docs.openshift.com/container-platform/4.11/openshift_images/create-images.html#use-uid_create-images
RUN mkdir /.npm
RUN chgrp -R 0 /.npm && \
    chmod -R g=u /.npm
CMD ["npm", "run", "start"]
