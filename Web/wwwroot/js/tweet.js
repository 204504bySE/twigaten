Vue.component('a-unixtime', {
    props: ['href', 'unixtime'],
    computed: {
        localtime: function () {
            const local = new Date(this.unixtime * 1000);
            return local.getFullYear() + '-' + (local.getMonth() + 1) + '-' + local.getDate() + ' ' + local.getHours() + ':' + local.getMinutes() + ':' + local.getSeconds();
        }
    },
    template: `<a v-bind:href="href">{{ localtime }}</a>`
});
