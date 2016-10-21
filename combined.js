'use strict';

var angularAPP = angular.module('angularAPP', [
  'ui.ace',
  'angularSpinner',
  'angularUtils.directives.dirPagination',
  'ngRoute',
  'ngMaterial',
  'ngAnimate',
  'md.data.table',
  'ngAria',
  'base64',
  'ngOboe',
  'ui.grid',
  'ui.grid.resizeColumns',
  'angular-json-tree'
]);

angularAPP.controller('HeaderCtrl', function ($rootScope, $scope, $log) {
   $rootScope.showList = true;
   $rootScope.toggleList = function () {
      $rootScope.showList = !$rootScope.showList;
   };

   $rootScope.showLeftList = function () {
      $rootScope.showList = true;
   };


});

angularAPP.config(function ($routeProvider) {
  $routeProvider
    .when('/', {
      templateUrl: 'src/kafka-topics/home/home.html',
      controller: 'HomeCtrl'
    })
    .when('/create-topic', {
      templateUrl: 'src/kafka-topics/new/new-topic.html',
      controller: 'HeaderCtrl'
    })
    .when('/topic/:topicCategoryUrl/:topicName/', {
        templateUrl: 'src/kafka-topics/view/view.html',
        controller: 'ViewTopicCtrl'
      })
    .when('/topic/:topicCategoryUrl/:topicName/:selectedTabIndex', {
      templateUrl: 'src/kafka-topics/view/view.html',
      controller: 'ViewTopicCtrl'
    }).otherwise({
    redirectTo: '/'
  });
  // $locationProvider.html5Mode(true);
});

angularAPP.run(['$route', '$rootScope', '$location', function ($route, $rootScope, $location) {
    var original = $location.path;
    $location.path = function (path, reload) {
        if (reload === false) {
            var lastRoute = $route.current;
            var un = $rootScope.$on('$locationChangeSuccess', function () {
                $route.current = lastRoute;
                un();
            });
        }
        return original.apply($location, [path]);
    };
}])

// ng-show="x | isEmpty"
angularAPP.filter('isEmpty', function () {
  var bar;
  return function (obj) {
    for (bar in obj) {
      if (obj.hasOwnProperty(bar)) {
        return false;
      }
    }
    return true;
  };
});

angularAPP.filter("sanitize", ['$sce', function ($sce) {
  return function (htmlCode) {
    return $sce.trustAsHtml(htmlCode);
  }
}]);

angularAPP.config(function ($mdThemingProvider) {
  $mdThemingProvider.theme('default')
    .primaryPalette('blue-grey')
    .accentPalette('blue')
    .warnPalette('grey');
});

// Replace with the URL where a Kafka REST service is listening
var KAFKA_REST = "http://localhost:8082";// "https://kafka-rest-proxy.demo.landoop.com"

// UI to your `schema-registry-ui` app
var UI_SCHEMA_REGISTRY = "http://localhost:8081"; // Leave empty to disable integration - // http://schema-registry-ui.landoop.com

var KAFKA_TOPIC_DELETE_COMMAND = "kafka-topics --zookeeper zookeeper-host:2181/confluent --delete --topic";

var KAFKA_REST_ENV = {

  // Sets the defaul maximum amount of bytes to fetch from each topic
  MAX_BYTES: "?max_bytes=10000000",

  // Pre-configure the Data Type on particular well-known topics
  JSON_TOPICS: ["_schemas"],

  BINARY_TOPICS: ["connect-configs", "connect-offsets", "connect-status", "__consumer_offsets", "_confluent-monitoring", "_confluent-controlcenter", "__confluent.support.metr"],

  // If a topic starts with this particular prefix - it's a control topic
  CONTROL_TOPICS: ["_confluent-controlcenter", "connect-configs", "connect-offsets", "__confluent", "__consumer_offsets", "_confluent-monitoring"],

};

var KAFKA_DEFAULTS =
  [{
    "property": "cleanup.policy",
    "default": "delete",
    "serverDefaultProperties": "log.cleanup.policy",
    "description": "A string that is either \"delete\" or \"compact\". This string designates the retention policy to use on old log segments. The default policy (\"delete\") will discard old segments when their retention time or size limit has been reached. The \"compact\" setting will enable log compaction on the topic."
  }, {
    "property": "delete.retention.ms",
    "default": "86400000",
    "serverDefaultProperties": "log.cleaner.delete.retention.ms",
    "description": "The amount of time to retain delete tombstone markers for log compacted topics. This setting also gives a bound on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete tombstones may be collected before they complete their scan). Default is 24 hours"
  }, {
    "property": "flush.messages",
    "default": "None",
    "serverDefaultProperties": "log.flush.interval.messages",
    "description": "This setting allows specifying an interval at which we will force an fsync of data written to the log. For example if this was set to 1 we would fsync after every message; if it were 5 we would fsync after every five messages. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient. This setting can be overridden on a per-topic basis (see the per-topic configuration section)."
  }, {
    "property": "flush.ms",
    "default": "None",
    "serverDefaultProperties": "log.flush.interval.ms",
    "description": "This setting allows specifying a time interval at which we will force an fsync of data written to the log. For example if this was set to 1000 we would fsync after 1000 ms had passed. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient."
  }, {
    "property": "index.interval.bytes",
    "default": "4096",
    "serverDefaultProperties": "log.index.interval.bytes",
    "description": "This setting controls how frequently Kafka adds an index entry to it's offset index. The default setting ensures that we index a message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact position in the log but makes the index larger. You probably don't need to change this."
  }, {
    "property": "max.message.bytes",
    "default": "10000000",
    "serverDefaultProperties": "message.max.bytes",
    "description": "This is largest message size Kafka will allow to be appended to this topic. Note that if you increase this size you must also increase your consumer's fetch size so they can fetch messages this large."
  }, {
    "property": "min.cleanable.dirty.ratio",
    "default": "0.5",
    "serverDefaultProperties": "log.cleaner.min.cleanable.ratio",
    "description": "This configuration controls how frequently the log compactor will attempt to clean the log (assuming log compaction is enabled). By default we will avoid cleaning a log where more than 50% of the log has been compacted. This ratio bounds the maximum space wasted in the log by duplicates (at 50% at most 50% of the log could be duplicates). A higher ratio will mean fewer, more efficient cleanings but will mean more wasted space in the log."
  }, {
    "property": "min.insync.replicas",
    "default": "1",
    "serverDefaultProperties": "min.insync.replicas",
    "description": "When a producer sets acks to \"all\", min.insync.replicas specifies the minimum number of replicas that must acknowledge a write for the write to be considered successful. If this minimum cannot be met, then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend). When used together, min.insync.replicas and acks allow you to enforce greater durability guarantees. A typical scenario would be to create a topic with a replication factor of 3, set min.insync.replicas to 2, and produce with acks of \"all\". This will ensure that the producer raises an exception if a majority of replicas do not receive a write."
  }, {
    "property": "retention.bytes",
    "default": "None",
    "serverDefaultProperties": "log.retention.bytes",
    "description": "This configuration controls the maximum size a log can grow to before we will discard old log segments to free up space if we are using the \"delete\" retention policy. By default there is no size limit only a time limit."
  }, {
    "property": "retention.ms",
    "default": "7 days",
    "serverDefaultProperties": "log.retention.minutes",
    "description": "This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the \"delete\" retention policy. This represents an SLA on how soon consumers must read their data."
  }, {
    "property": "segment.bytes",
    "default": "1 GB",
    "serverDefaultProperties": "log.segment.bytes",
    "description": "This configuration controls the segment file size for the log. Retention and cleaning is always done a file at a time so a larger segment size means fewer files but less granular control over retention."
  }, {
    "property": "segment.index.bytes",
    "default": "10 MB",
    "serverDefaultProperties": "log.index.size.max.bytes",
    "description": "This configuration controls the size of the index that maps offsets to file positions. We preallocate this index file and shrink it only after log rolls. You generally should not need to change this setting."
  }, {
    "property": "segment.ms",
    "default": "7 days",
    "serverDefaultProperties": "log.roll.hours",
    "description": "This configuration controls the period of time after which Kafka will force the log to roll even if the segment file isn't full to ensure that retention can delete or compact old data."
  }, {
    "property": "segment.jitter.ms",
    "default": "0",
    "serverDefaultProperties": "log.roll.jitter.{ms,hours}",
    "description": "The maximum jitter to subtract from logRollTimeMillis."
  }];

/**
 * dirPagination - AngularJS module for paginating (almost) anything.
 *
 *
 * Credits
 * =======
 *
 * Daniel Tabuenca: https://groups.google.com/d/msg/angular/an9QpzqIYiM/r8v-3W1X5vcJ
 * for the idea on how to dynamically invoke the ng-repeat directive.
 *
 * I borrowed a couple of lines and a few attribute names from the AngularUI Bootstrap project:
 * https://github.com/angular-ui/bootstrap/blob/master/src/pagination/pagination.js
 *
 * Copyright 2014 Michael Bromley <michael@michaelbromley.co.uk>
 *
 * @antonios 18-Aug-16 enhanced this with a material-design pagination template
 */

(function() {

    /**
     * Config
     */
    var moduleName = 'angularUtils.directives.dirPagination';
    var DEFAULT_ID = '__default';

    /**
     * Module
     */
    angular.module(moduleName, [])
        .directive('dirPaginate', ['$compile', '$parse', 'paginationService', dirPaginateDirective])
        .directive('dirPaginateNoCompile', noCompileDirective)
        .directive('dirPaginationControls', ['paginationService', 'paginationTemplate', dirPaginationControlsDirective])
        .filter('itemsPerPage', ['paginationService', itemsPerPageFilter])
        .service('paginationService', paginationService)
        .provider('paginationTemplate', paginationTemplateProvider)
        .run(['$templateCache',dirPaginationControlsTemplateInstaller]);

    function dirPaginateDirective($compile, $parse, paginationService) {

        return  {
            terminal: true,
            multiElement: true,
            priority: 100,
            compile: dirPaginationCompileFn
        };

        function dirPaginationCompileFn(tElement, tAttrs){

            var expression = tAttrs.dirPaginate;
            // regex taken directly from https://github.com/angular/angular.js/blob/v1.4.x/src/ng/directive/ngRepeat.js#L339
            var match = expression.match(/^\s*([\s\S]+?)\s+in\s+([\s\S]+?)(?:\s+as\s+([\s\S]+?))?(?:\s+track\s+by\s+([\s\S]+?))?\s*$/);

            var filterPattern = /\|\s*itemsPerPage\s*:\s*(.*\(\s*\w*\)|([^\)]*?(?=\s+as\s+))|[^\)]*)/;
            if (match[2].match(filterPattern) === null) {
                throw 'pagination directive: the \'itemsPerPage\' filter must be set.';
            }
            var itemsPerPageFilterRemoved = match[2].replace(filterPattern, '');
            var collectionGetter = $parse(itemsPerPageFilterRemoved);

            addNoCompileAttributes(tElement);

            // If any value is specified for paginationId, we register the un-evaluated expression at this stage for the benefit of any
            // dir-pagination-controls directives that may be looking for this ID.
            var rawId = tAttrs.paginationId || DEFAULT_ID;
            paginationService.registerInstance(rawId);

            return function dirPaginationLinkFn(scope, element, attrs){

                // Now that we have access to the `scope` we can interpolate any expression given in the paginationId attribute and
                // potentially register a new ID if it evaluates to a different value than the rawId.
                var paginationId = $parse(attrs.paginationId)(scope) || attrs.paginationId || DEFAULT_ID;

                // (TODO: this seems sound, but I'm reverting as many bug reports followed it's introduction in 0.11.0.
                // Needs more investigation.)
                // In case rawId != paginationId we deregister using rawId for the sake of general cleanliness
                // before registering using paginationId
                // paginationService.deregisterInstance(rawId);
                paginationService.registerInstance(paginationId);

                var repeatExpression = getRepeatExpression(expression, paginationId);
                addNgRepeatToElement(element, attrs, repeatExpression);

                removeTemporaryAttributes(element);
                var compiled =  $compile(element);

                var currentPageGetter = makeCurrentPageGetterFn(scope, attrs, paginationId);
                paginationService.setCurrentPageParser(paginationId, currentPageGetter, scope);

                if (typeof attrs.totalItems !== 'undefined') {
                    paginationService.setAsyncModeTrue(paginationId);
                    scope.$watch(function() {
                        return $parse(attrs.totalItems)(scope);
                    }, function (result) {
                        if (0 <= result) {
                            paginationService.setCollectionLength(paginationId, result);
                        }
                    });
                } else {
                    paginationService.setAsyncModeFalse(paginationId);
                    scope.$watchCollection(function() {
                        return collectionGetter(scope);
                    }, function(collection) {
                        if (collection) {
                            var collectionLength = (collection instanceof Array) ? collection.length : Object.keys(collection).length;
                            paginationService.setCollectionLength(paginationId, collectionLength);
                        }
                    });
                }

                // Delegate to the link function returned by the new compilation of the ng-repeat
                compiled(scope);

                // (TODO: Reverting this due to many bug reports in v 0.11.0. Needs investigation as the
                // principle is sound)
                // When the scope is destroyed, we make sure to remove the reference to it in paginationService
                // so that it can be properly garbage collected
                // scope.$on('$destroy', function destroyDirPagination() {
                //     paginationService.deregisterInstance(paginationId);
                // });
            };
        }

        /**
         * If a pagination id has been specified, we need to check that it is present as the second argument passed to
         * the itemsPerPage filter. If it is not there, we add it and return the modified expression.
         *
         * @param expression
         * @param paginationId
         * @returns {*}
         */
        function getRepeatExpression(expression, paginationId) {
            var repeatExpression,
                idDefinedInFilter = !!expression.match(/(\|\s*itemsPerPage\s*:[^|]*:[^|]*)/);

            if (paginationId !== DEFAULT_ID && !idDefinedInFilter) {
                repeatExpression = expression.replace(/(\|\s*itemsPerPage\s*:\s*[^|\s]*)/, "$1 : '" + paginationId + "'");
            } else {
                repeatExpression = expression;
            }

            return repeatExpression;
        }

        /**
         * Adds the ng-repeat directive to the element. In the case of multi-element (-start, -end) it adds the
         * appropriate multi-element ng-repeat to the first and last element in the range.
         * @param element
         * @param attrs
         * @param repeatExpression
         */
        function addNgRepeatToElement(element, attrs, repeatExpression) {
            if (element[0].hasAttribute('dir-paginate-start') || element[0].hasAttribute('data-dir-paginate-start')) {
                // using multiElement mode (dir-paginate-start, dir-paginate-end)
                attrs.$set('ngRepeatStart', repeatExpression);
                element.eq(element.length - 1).attr('ng-repeat-end', true);
            } else {
                attrs.$set('ngRepeat', repeatExpression);
            }
        }

        /**
         * Adds the dir-paginate-no-compile directive to each element in the tElement range.
         * @param tElement
         */
        function addNoCompileAttributes(tElement) {
            angular.forEach(tElement, function(el) {
                if (el.nodeType === 1) {
                    angular.element(el).attr('dir-paginate-no-compile', true);
                }
            });
        }

        /**
         * Removes the variations on dir-paginate (data-, -start, -end) and the dir-paginate-no-compile directives.
         * @param element
         */
        function removeTemporaryAttributes(element) {
            angular.forEach(element, function(el) {
                if (el.nodeType === 1) {
                    angular.element(el).removeAttr('dir-paginate-no-compile');
                }
            });
            element.eq(0).removeAttr('dir-paginate-start').removeAttr('dir-paginate').removeAttr('data-dir-paginate-start').removeAttr('data-dir-paginate');
            element.eq(element.length - 1).removeAttr('dir-paginate-end').removeAttr('data-dir-paginate-end');
        }

        /**
         * Creates a getter function for the current-page attribute, using the expression provided or a default value if
         * no current-page expression was specified.
         *
         * @param scope
         * @param attrs
         * @param paginationId
         * @returns {*}
         */
        function makeCurrentPageGetterFn(scope, attrs, paginationId) {
            var currentPageGetter;
            if (attrs.currentPage) {
                currentPageGetter = $parse(attrs.currentPage);
            } else {
                // If the current-page attribute was not set, we'll make our own.
                // Replace any non-alphanumeric characters which might confuse
                // the $parse service and give unexpected results.
                // See https://github.com/michaelbromley/angularUtils/issues/233
                var defaultCurrentPage = '_'+(paginationId + '__currentPage').replace(/\W/g, '_');
                scope[defaultCurrentPage] = 1;
                currentPageGetter = $parse(defaultCurrentPage);
            }
            return currentPageGetter;
        }
    }

    /**
     * This is a helper directive that allows correct compilation when in multi-element mode (ie dir-paginate-start, dir-paginate-end).
     * It is dynamically added to all elements in the dir-paginate compile function, and it prevents further compilation of
     * any inner directives. It is then removed in the link function, and all inner directives are then manually compiled.
     */
    function noCompileDirective() {
        return {
            priority: 5000,
            terminal: true
        };
    }

    function dirPaginationControlsTemplateInstaller($templateCache) {
      var strVar="";
      strVar += "<section  layout=\"row\" layout-align=\"center\" ng-if=\"1 < pages.length || !autoHide\" class=\"pagination\"> <md-button aria-label=\"Previous page\" ng-if=\"boundaryLinks\" ng-disabled=\"pagination.current===1\" ng-click=\"setCurrent(1)\"> <ng-md-icon icon=\"first_page\"><\/ng-md-icon> <\/md-button> <md-button aria-label=\"First page\" ng-if=\"directionLinks\" ng-disabled=\"pagination.current===1\" ng-click=\"setCurrent(pagination.current - 1)\"> <ng-md-icon class=\"fa fa-chevron-left\"><\/ng-md-icon> <\/md-button> <md-button ng-repeat=\"pageNumber in pages track by tracker(pageNumber, $index)\" ng-class=\"{'md-primary' : pagination.current==pageNumber}\" ng-disabled=\"pageNumber==='...'\" ng-click=\"setCurrent(pageNumber)\">{{pageNumber}}<\/md-button> <md-button aria-label=\"Last page\" ng-if=\"directionLinks\" ng-disabled=\"pagination.current===pagination.last\" ng-click=\"setCurrent(pagination.current + 1)\"> <ng-md-icon class=\"fa fa-chevron-right\"><\/ng-md-icon> <\/md-button> <md-button ng-if=\"boundaryLinks\" ng-disabled=\"pagination.current===pagination.last\" ng-click=\"setCurrent(pagination.last)\"> <ng-md-icon icon=\"last_page\"><\/ng-md-icon> <\/md-button><\/section>";

      $templateCache.put('angularUtils.directives.dirPagination.template', strVar);
    }

    function dirPaginationControlsDirective(paginationService, paginationTemplate) {

        var numberRegex = /^\d+$/;

        var DDO = {
            restrict: 'AE',
            scope: {
                maxSize: '=?',
                onPageChange: '&?',
                paginationId: '=?',
                autoHide: '=?'
            },
            link: dirPaginationControlsLinkFn
        };

        // We need to check the paginationTemplate service to see whether a template path or
        // string has been specified, and add the `template` or `templateUrl` property to
        // the DDO as appropriate. The order of priority to decide which template to use is
        // (highest priority first):
        // 1. paginationTemplate.getString()
        // 2. attrs.templateUrl
        // 3. paginationTemplate.getPath()
        var templateString = paginationTemplate.getString();
        if (templateString !== undefined) {
            DDO.template = templateString;
        } else {
            DDO.templateUrl = function(elem, attrs) {
                return attrs.templateUrl || paginationTemplate.getPath();
            };
        }
        return DDO;

        function dirPaginationControlsLinkFn(scope, element, attrs) {

            // rawId is the un-interpolated value of the pagination-id attribute. This is only important when the corresponding dir-paginate directive has
            // not yet been linked (e.g. if it is inside an ng-if block), and in that case it prevents this controls directive from assuming that there is
            // no corresponding dir-paginate directive and wrongly throwing an exception.
            var rawId = attrs.paginationId ||  DEFAULT_ID;
            var paginationId = scope.paginationId || attrs.paginationId ||  DEFAULT_ID;

            if (!paginationService.isRegistered(paginationId) && !paginationService.isRegistered(rawId)) {
                var idMessage = (paginationId !== DEFAULT_ID) ? ' (id: ' + paginationId + ') ' : ' ';
                if (window.console) {
                    console.warn('Pagination directive: the pagination controls' + idMessage + 'cannot be used without the corresponding pagination directive, which was not found at link time.');
                }
            }

            if (!scope.maxSize) { scope.maxSize = 9; }
            scope.autoHide = scope.autoHide === undefined ? true : scope.autoHide;
            scope.directionLinks = angular.isDefined(attrs.directionLinks) ? scope.$parent.$eval(attrs.directionLinks) : true;
            scope.boundaryLinks = angular.isDefined(attrs.boundaryLinks) ? scope.$parent.$eval(attrs.boundaryLinks) : false;

            var paginationRange = Math.max(scope.maxSize, 5);
            scope.pages = [];
            scope.pagination = {
                last: 1,
                current: 1
            };
            scope.range = {
                lower: 1,
                upper: 1,
                total: 1
            };

            scope.$watch('maxSize', function(val) {
                if (val) {
                    paginationRange = Math.max(scope.maxSize, 5);
                    generatePagination();
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return (paginationService.getCollectionLength(paginationId) + 1) * paginationService.getItemsPerPage(paginationId);
                }
            }, function(length) {
                if (0 < length) {
                    generatePagination();
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return (paginationService.getItemsPerPage(paginationId));
                }
            }, function(current, previous) {
                if (current != previous && typeof previous !== 'undefined') {
                    goToPage(scope.pagination.current);
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return paginationService.getCurrentPage(paginationId);
                }
            }, function(currentPage, previousPage) {
                if (currentPage != previousPage) {
                    goToPage(currentPage);
                }
            });

            scope.setCurrent = function(num) {
                if (paginationService.isRegistered(paginationId) && isValidPageNumber(num)) {
                    num = parseInt(num, 10);
                    paginationService.setCurrentPage(paginationId, num);
                }
            };

            /**
             * Custom "track by" function which allows for duplicate "..." entries on long lists,
             * yet fixes the problem of wrongly-highlighted links which happens when using
             * "track by $index" - see https://github.com/michaelbromley/angularUtils/issues/153
             * @param id
             * @param index
             * @returns {string}
             */
            scope.tracker = function(id, index) {
                return id + '_' + index;
            };

            function goToPage(num) {
                if (paginationService.isRegistered(paginationId) && isValidPageNumber(num)) {
                    var oldPageNumber = scope.pagination.current;

                    scope.pages = generatePagesArray(num, paginationService.getCollectionLength(paginationId), paginationService.getItemsPerPage(paginationId), paginationRange);
                    scope.pagination.current = num;
                    updateRangeValues();

                    // if a callback has been set, then call it with the page number as the first argument
                    // and the previous page number as a second argument
                    if (scope.onPageChange) {
                        scope.onPageChange({
                            newPageNumber : num,
                            oldPageNumber : oldPageNumber
                        });
                    }
                }
            }

            function generatePagination() {
                if (paginationService.isRegistered(paginationId)) {
                    var page = parseInt(paginationService.getCurrentPage(paginationId)) || 1;
                    scope.pages = generatePagesArray(page, paginationService.getCollectionLength(paginationId), paginationService.getItemsPerPage(paginationId), paginationRange);
                    scope.pagination.current = page;
                    scope.pagination.last = scope.pages[scope.pages.length - 1];
                    if (scope.pagination.last < scope.pagination.current) {
                        scope.setCurrent(scope.pagination.last);
                    } else {
                        updateRangeValues();
                    }
                }
            }

            /**
             * This function updates the values (lower, upper, total) of the `scope.range` object, which can be used in the pagination
             * template to display the current page range, e.g. "showing 21 - 40 of 144 results";
             */
            function updateRangeValues() {
                if (paginationService.isRegistered(paginationId)) {
                    var currentPage = paginationService.getCurrentPage(paginationId),
                        itemsPerPage = paginationService.getItemsPerPage(paginationId),
                        totalItems = paginationService.getCollectionLength(paginationId);

                    scope.range.lower = (currentPage - 1) * itemsPerPage + 1;
                    scope.range.upper = Math.min(currentPage * itemsPerPage, totalItems);
                    scope.range.total = totalItems;
                }
            }
            function isValidPageNumber(num) {
                return (numberRegex.test(num) && (0 < num && num <= scope.pagination.last));
            }
        }

        /**
         * Generate an array of page numbers (or the '...' string) which is used in an ng-repeat to generate the
         * links used in pagination
         *
         * @param currentPage
         * @param rowsPerPage
         * @param paginationRange
         * @param collectionLength
         * @returns {Array}
         */
        function generatePagesArray(currentPage, collectionLength, rowsPerPage, paginationRange) {
            var pages = [];
            var totalPages = Math.ceil(collectionLength / rowsPerPage);
            var halfWay = Math.ceil(paginationRange / 2);
            var position;

            if (currentPage <= halfWay) {
                position = 'start';
            } else if (totalPages - halfWay < currentPage) {
                position = 'end';
            } else {
                position = 'middle';
            }

            var ellipsesNeeded = paginationRange < totalPages;
            var i = 1;
            while (i <= totalPages && i <= paginationRange) {
                var pageNumber = calculatePageNumber(i, currentPage, paginationRange, totalPages);

                var openingEllipsesNeeded = (i === 2 && (position === 'middle' || position === 'end'));
                var closingEllipsesNeeded = (i === paginationRange - 1 && (position === 'middle' || position === 'start'));
                if (ellipsesNeeded && (openingEllipsesNeeded || closingEllipsesNeeded)) {
                    pages.push('...');
                } else {
                    pages.push(pageNumber);
                }
                i ++;
            }
            return pages;
        }

        /**
         * Given the position in the sequence of pagination links [i], figure out what page number corresponds to that position.
         *
         * @param i
         * @param currentPage
         * @param paginationRange
         * @param totalPages
         * @returns {*}
         */
        function calculatePageNumber(i, currentPage, paginationRange, totalPages) {
            var halfWay = Math.ceil(paginationRange/2);
            if (i === paginationRange) {
                return totalPages;
            } else if (i === 1) {
                return i;
            } else if (paginationRange < totalPages) {
                if (totalPages - halfWay < currentPage) {
                    return totalPages - paginationRange + i;
                } else if (halfWay < currentPage) {
                    return currentPage - halfWay + i;
                } else {
                    return i;
                }
            } else {
                return i;
            }
        }
    }

    /**
     * This filter slices the collection into pages based on the current page number and number of items per page.
     * @param paginationService
     * @returns {Function}
     */
    function itemsPerPageFilter(paginationService) {

        return function(collection, itemsPerPage, paginationId) {
            if (typeof (paginationId) === 'undefined') {
                paginationId = DEFAULT_ID;
            }
            if (!paginationService.isRegistered(paginationId)) {
                throw 'pagination directive: the itemsPerPage id argument (id: ' + paginationId + ') does not match a registered pagination-id.';
            }
            var end;
            var start;
            if (angular.isObject(collection)) {
                itemsPerPage = parseInt(itemsPerPage) || 9999999999;
                if (paginationService.isAsyncMode(paginationId)) {
                    start = 0;
                } else {
                    start = (paginationService.getCurrentPage(paginationId) - 1) * itemsPerPage;
                }
                end = start + itemsPerPage;
                paginationService.setItemsPerPage(paginationId, itemsPerPage);

                if (collection instanceof Array) {
                    // the array just needs to be sliced
                    return collection.slice(start, end);
                } else {
                    // in the case of an object, we need to get an array of keys, slice that, then map back to
                    // the original object.
                    var slicedObject = {};
                    angular.forEach(keys(collection).slice(start, end), function(key) {
                        slicedObject[key] = collection[key];
                    });
                    return slicedObject;
                }
            } else {
                return collection;
            }
        };
    }

    /**
     * Shim for the Object.keys() method which does not exist in IE < 9
     * @param obj
     * @returns {Array}
     */
    function keys(obj) {
        if (!Object.keys) {
            var objKeys = [];
            for (var i in obj) {
                if (obj.hasOwnProperty(i)) {
                    objKeys.push(i);
                }
            }
            return objKeys;
        } else {
            return Object.keys(obj);
        }
    }

    /**
     * This service allows the various parts of the module to communicate and stay in sync.
     */
    function paginationService() {

        var instances = {};
        var lastRegisteredInstance;

        this.registerInstance = function(instanceId) {
            if (typeof instances[instanceId] === 'undefined') {
                instances[instanceId] = {
                    asyncMode: false
                };
                lastRegisteredInstance = instanceId;
            }
        };

        this.deregisterInstance = function(instanceId) {
            delete instances[instanceId];
        };

        this.isRegistered = function(instanceId) {
            return (typeof instances[instanceId] !== 'undefined');
        };

        this.getLastInstanceId = function() {
            return lastRegisteredInstance;
        };

        this.setCurrentPageParser = function(instanceId, val, scope) {
            instances[instanceId].currentPageParser = val;
            instances[instanceId].context = scope;
        };
        this.setCurrentPage = function(instanceId, val) {
            instances[instanceId].currentPageParser.assign(instances[instanceId].context, val);
        };
        this.getCurrentPage = function(instanceId) {
            var parser = instances[instanceId].currentPageParser;
            return parser ? parser(instances[instanceId].context) : 1;
        };

        this.setItemsPerPage = function(instanceId, val) {
            instances[instanceId].itemsPerPage = val;
        };
        this.getItemsPerPage = function(instanceId) {
            return instances[instanceId].itemsPerPage;
        };

        this.setCollectionLength = function(instanceId, val) {
            instances[instanceId].collectionLength = val;
        };
        this.getCollectionLength = function(instanceId) {
            return instances[instanceId].collectionLength;
        };

        this.setAsyncModeTrue = function(instanceId) {
            instances[instanceId].asyncMode = true;
        };

        this.setAsyncModeFalse = function(instanceId) {
            instances[instanceId].asyncMode = false;
        };

        this.isAsyncMode = function(instanceId) {
            return instances[instanceId].asyncMode;
        };
    }

    /**
     * This provider allows global configuration of the template path used by the dir-pagination-controls directive.
     */
    function paginationTemplateProvider() {

        var templatePath = 'angularUtils.directives.dirPagination.template';
        var templateString;

        /**
         * Set a templateUrl to be used by all instances of <dir-pagination-controls>
         * @param {String} path
         */
        this.setPath = function(path) {
            templatePath = path;
        };

        /**
         * Set a string of HTML to be used as a template by all instances
         * of <dir-pagination-controls>. If both a path *and* a string have been set,
         * the string takes precedence.
         * @param {String} str
         */
        this.setString = function(str) {
            templateString = str;
        };

        this.$get = function() {
            return {
                getPath: function() {
                    return templatePath;
                },
                getString: function() {
                    return templateString;
                }
            };
        };
    }
})();

/**
 * Kafka-Rest-Proxy angularJS Factory
 * version 0.7-SNAPSHOT (18.Aug.2016)
 *
 * @author antonios@landoop.com
 */
angularAPP.factory('KafkaRestProxyFactory', function ($rootScope, $http, $log, $base64, $q, Oboe, toastFactory) {

  // Topics
  var schemas;

  /**
   * Get all topic names
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--topics
   */
  function getTopicNames() {

    var url = KAFKA_REST + '/topics';
    $log.debug('  curl -X GET ' + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.get(url).then(
      function success(response) {
        $log.debug("  curl -X GET " + url + " in [ " + (new Date().getTime() - start) + " ] msec");
        var topicNames = response.data;

        deferred.resolve(topicNames);
      },
      function failure(response) {
        $log.error("Error in getting topics from kafka-rest : " + JSON.stringify(response));
        deferred.reject("Error in getting topics from kafka-rest");
      });

    return deferred.promise;

  }

  /**
   * Get topic metadata
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--topics-(string-topic_name)
   */
  function getTopicMetadata(topicName) {

    var url = KAFKA_REST + '/topics/' + topicName;
    $log.debug('  curl -X GET ' + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.get(url).then(
      function success(response) {
        $log.debug("  curl -X GET " + url + " in [ " + (new Date().getTime() - start) + " ] msec");
        var topicWithMetadata = response.data;
        deferred.resolve(topicWithMetadata);
      },
      function failure(response) {
        $log.error("Error in getting topics from kafka-rest : " + JSON.stringify(response));
        deferred.reject("Error in getting topics from kafka-rest");
      });

    return deferred.promise;

  }

  /**
   * Produce messages to a topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#post--topics-(string-topic_name)
   */
  /*** dummy implementation ***/
  function produceMessagesToTopic(topicName) {

    // Maybe we should i) auto-detect and ii) ask user which schema-registry ID to use
    // Key | Value schemas are optional
    var requestJsonObject = {
      //key_schema: "Full schema encoded as a string (e.g. JSON serialized for Avro data)",
      key_schema_id: 1,
      //value_schema: "Full schema encoded as a string (e.g. JSON serialized for Avro data)",
      value_schema_id: 2
    };

    var sampleRecord = {
      key: 1, //  (object) – The message key, formatted according to the embedded format, or null to omit a key (optional)
      value: 2, // (object) – The message value, formatted according to the embedded format
      partition: 1 // (int) – Partition to store the message in (optional)
    };

    var payload = [];
    payload.push(sampleRecord);

    // TODO ..
  }

  // Partitions

  /**
   * Get a list of partitions for the topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--topics-(string-topic_name)-partitions
   */
  function getPartitions(topicName) {

    var url = KAFKA_REST + '/topics/' + topicName + '/partitions';
    $log.debug('  curl -X GET ' + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.get(url).then(
      function success(response) {
        $log.debug("  curl -X GET " + url + " in [ " + (new Date().getTime() - start) + " ] msec");
        var partitions = response.data;
        deferred.resolve(partitions);
      },
      function failure(response, status) {
        var msg = "Error in getting partitions of topic " + topicName;
        if (status == 404)
          msg = msg + " Topic does not exist";
        $log.error(msg);
        deferred.reject(msg);
      });

    return deferred.promise;

  }

  /**
   * Get metadata about a particular partition
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--topics-(string-topic_name)-partitions-(int-partition_id)
   */
  function getPartitionMetadata(topicName, partitionID) {

    var url = KAFKA_REST + '/topics/' + topicName + '/partitions/' + partitionID;
    $log.debug('  curl -X GET ' + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.get(url).then(
      function success(response) {
        $log.debug("  curl -X GET " + url + " in [ " + (new Date().getTime() - start) + " ] msec");
        var partitionMetadata = response.data;
        deferred.resolve(partitionMetadata);
      },
      function failure(response, status) {
        var msg = "Error in getting partition [" + partitionID + "] metadata of topic [" + topicName + "]";
        if (status == 404)
          msg = msg + " Topic or partition does not exist";
        $log.error(msg);
        deferred.reject(msg);
      });

    return deferred.promise;

  }

  /**
   * Consume messages from one partition of the topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--topics-(string-topic_name)-partitions-(int-partition_id)-messages?offset=(int)[&count=(int)]
   */
  function consumeMessagesFromPartition(topicName, partitionID, offset, count) {

    var url = KAFKA_REST + '/topics/' + topicName + '/partitions/' + partitionID +
      '/messages?offset=' + offset + '&count=' + count; // offset and count (int)

    // PENDING IMPLEMENTATION ..

  }

  /**
   * Produce messages to one partition of the topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#post--topics-(string-topic_name)-partitions-(int-partition_id)
   */
  function produceMessagesToPartition(topicName, partitionID, offset, count) {

    var url = KAFKA_REST + '/topics/' + topicName + '/partitions/' + partitionID;

    // POST
    // PENDING IMPLEMENTATION ..

  }

  // Consumers

  /**
   * Create a new consumer instance in the consumer group.
   *
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#post--consumers-(string-group_name)
   */
  function createNewConsumer(consumerGroup, consumerName, format, autoOffsetReset, enableAutoCommit) {

    var url = KAFKA_REST + '/consumers/' + consumerName;
    $log.info("Creating Kafka Rest consumer for " + format + " data");

    $rootScope.allCurlCommands = "";
    var instance = "instance"; // For creating new --from-beginning

    // @formatter:off
    function getContentType(format) {
      switch (format) {
        case 'avro': return 'application/vnd.kafka.v1+json';
        case 'json': return 'application/vnd.kafka.v1+json';
        case 'binary': return 'application/vnd.kafka.binary.v1+json';
        default: $log.error("Unsupported consumer format : " + format);
      }
    }
    // @formatter:on

    var data = '{"name": "' + instance + '", "format": "' + format + '", "auto.offset.reset": "smallest"}';
    var messageContentType = getContentType(format);
    var postCreateConsumer = {
      method: 'POST',
      url: url,
      data: data,
      headers: {'Content-Type': messageContentType}
    };
    var curlCreateConsumer = 'curl -X POST -H "Content-Type: ' + messageContentType + '" ' + "--data '" + data + "' " + url;
    $log.debug("  " + curlCreateConsumer);

    // Create a consumer and fetch data
    var deferred = $q.defer();
    $http(postCreateConsumer).then(
      function success(response) {
        $log.info("Success in creating " + format + " consumer. instance_id = " + response.data.instance_id + " base_uri = " + response.data.base_uri);
        $rootScope.allCurlCommands = $rootScope.allCurlCommands + "\n" +
          "// Creating " + format + " consumer\n" + curlCreateConsumer + "\n";
        deferred.resolve(response.data);
      },
      function failure(response, statusText) {
        var msg = response.data.message;
        if (response.status == 409) msg = "409 " + msg;
        $log.warn(msg);
        deferred.reject();
      }
    );

    return deferred.promise;
  }

  /**
   *
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#post--consumers-(string-group_name)-instances-(string-instance)-offsets
   */
  function commitOffsetForConsumer(consumerName, instance) {

    var url = KAFKA_REST + '/consumers/' + consumerName + '/instances/' + instance + '/offsets';
    var postCommitOffsets = {
      method: 'POST',
      url: url,
      headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
    };
    $log.debug('  curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" ' + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http(postCommitOffsets).then(
      function success(response) {
        $log.debug("  curl -X POST " + url + " in [" + (new Date().getTime() - start) + " ] msec");
        $log.debug(response);
        deferred.resolve(response);
      },
      function failure(response, statusText) {
        $log.error(response);
        if (response.status == 409) {
          $log.info("409 detected! " + response.data.message);
          // toastFactory.showSimpleToast(response.data.message);
        }
      }
    );

    return deferred.promise;

  }

  /**
   * Delete consumer instance
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#delete--consumers-(string-group_name)-instances-(string-instance)
   */
  function deleteConsumerInstance(consumerName) {

    var url = KAFKA_REST + '/consumers/' + consumerName + '/instances/instance';
    var curlDeleteConsumer = '  curl -X DELETE ' + url;
    $log.debug(curlDeleteConsumer);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.delete(url).then(
      function successCallback(response) {
        $rootScope.allCurlCommands = $rootScope.allCurlCommands + "\n" +
          "// Deleting consumer \ncurl -X DELETE " + curlDeleteConsumer + "\n";
        $log.debug("  curl -X DELETE " + url + " in [ " + (new Date().getTime() - start) + "] msec");
        deferred.resolve(response.data);
      },
      function failure(error) {
        var msg = "Error in deleting consumer : " + JSON.stringify(error);
        $log.error(msg);
        deferred.reject(msg);
      }
    );

    return deferred.promise;

  }

  /**
   * Consume messages from a topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--consumers-(string-group_name)-instances-(string-instance)-topics-(string-topic_name)
   */
  function consumeMessagesFromTopic(consumerName, instanceName, topicName, format) {

    // instanceName is usually hard-code to 'instance'
    var url = KAFKA_REST + '/consumers/' + consumerName + '/instances/' + instanceName + '/topics/' + topicName + KAFKA_REST_ENV.MAX_BYTES;
    if (['avro', 'json', 'binary'].indexOf(format) < 0) {
      $log.error("Unsupported format [" + format + "]");
    }
    var acceptMessageType = 'application/vnd.kafka.' + format + '.v1+json';

    // Oboe - stream data in (roughly 1000 rows)
    var totals = 0;
    var curlGetData = 'curl -vs --stderr - -X GET -H "Accept: ' + acceptMessageType + '" ' + url;
    $log.debug("  " + curlGetData);
    var allResults = [];
    var start = new Date().getTime();
    $log.debug("Oboe-ing at " + url);

    var deferred = $q.defer();
    oboe({
      url: url,
      headers: {"Accept": acceptMessageType}
    })
    /* For every array item ..
     .node('!.*', function (values) {
     allResults.push(values);
     totals = totals + 1;
     var resultingTextData = "";
     if (messagetype == "binary") {
     var data2 = angular.forEach(data, function (d) {
     d.key = $base64.decode(values.key);
     d.value = $base64.decode(values.value);
     });
     resultingTextData = angular.toJson(data2, true);
     } else {
     resultingTextData = angular.toJson(values, true);
     }
     allResults.push(resultingTextData);
     // $scope.aceString = $scope.aceString +"\n" + values;
     if (totals < 3) {
     //  {"key":0,"value":{"itemID":6,"storeCode":"Ashford-New-Rents","count":100},"partition":0,"offset":1002760034}
     //  [{"key":null,"value":{"name":"testUser"},"partition":0,"offset":0}]
     $log.info(totals + " row => ", JSON.stringify(values));
     }
     if (totals == 1000) {
     var end = new Date().getTime();
     $log.info("[" + (end - start) + "] msec to fetch 1000 rows (now aborting)");
     deferred.resolve(allResults);
     this.abort();
     }
     })*/
      .done(function (things) {
        $rootScope.allCurlCommands = $rootScope.allCurlCommands + "\n" +
          "// Fetching " + format + " data\n" + curlGetData + "\n";

        deferred.resolve(things);

        // $rootScope.allCurlCommands = $rootScope.allCurlCommands + "\n" +
        //   "// Fetching " + messagetype + " data\n" + curlGetData + "\n";
        // var resultingTextData = "";
        // if (messagetype == "binary") {
        //   var data2 = angular.forEach(things, function (d) {
        //     d.key = $base64.decode(d.key);
        //     d.value = $base64.decode(d.value);
        //   });
        //   resultingTextData = angular.toJson(data2, true);
        // } else {
        //   resultingTextData = angular.toJson(things, true);
        // }
        // // $log.info("COMPLETED entire object " + JSON.stringify(things));
        // deferred.resolve(angular.toJson(things, true));
      })
      .fail(function () {
        var msg = "Failed consuming " + format + " data from topic " + topicName;
        $log.error(msg);
        deferred.reject(msg);
      });

    return deferred.promise;
  }

  // Brokers

  /**
   * Consume messages from a topic
   * @see http://docs.confluent.io/3.0.0/kafka-rest/docs/api.html#get--brokers
   */
  function getBrokers() {

    var url = KAFKA_REST + '/brokers';
    $log.debug("  curl -X GET " + url);
    var start = new Date().getTime();

    var deferred = $q.defer();
    $http.get(url).then(
      function success(response) {
        $log.debug("  curl -X GET " + url + " in [ " + (new Date().getTime() - start) + " ] msec with " + response.data.brokers.length + " brokers");
        deferred.resolve(response.data);
      },
      function failure(response) {
        $log.error("Failure with : " + JSON.stringify(response));
        deferred.reject();
      });

    return deferred.promise;

  }


  /**
   *
   * Some non API related methods
   *
   */

  function isControlTopic(topicName) {
    var isControlTopic = false;
    angular.forEach(KAFKA_REST_ENV.CONTROL_TOPICS, function (controlTopicPrefix) {
      if (topicName.startsWith(controlTopicPrefix, 0))
        isControlTopic = true;
    });
    return isControlTopic;
  }

  function isNormalTopic(topicName) {
    return !isControlTopic(topicName);
  }

  function bytesToSize(bytes) {
    var sizes = ['n/a', 'bytes', 'KBytes', 'MBytes', 'GBytes', 'TBytes', 'PBytes', 'EBytes', 'ZBytes', 'YBytes'];
    var i = +Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(i ? 1 : 0) + ' ' + sizes[isNaN(bytes) ? 0 : i + 1];
  }

  function shortenControlCenterName(topicName) {
    if (isControlTopic(topicName)) {
      return topicName
        .replace('_confluent-controlcenter-0-', '...')
        // .replace('aggregate-topic-partition', 'aggregate-topic')
        .replace('MonitoringMessageAggregatorWindows', 'monitor-msg')
        .replace('aggregatedTopicPartitionTableWindows', 'aggregate-window')
        .replace('monitoring-aggregate-rekey', 'monitor-rekey')
        .replace('MonitoringStream', 'monitor-stream')
        .replace('MonitoringVerifierStore', 'monitor-verifier')
        .replace('...Group', '...group')
        .replace('FIFTEEN_SECONDS', '15sec')
        .replace('ONE_HOUR', '1hour')
        .replace('ONE_WEEK', '1week');
    } else {
      return topicName;
    }
  }

  /**
   * Private method - part of pipeline (step 2)
   */
  function startFetchingData(format, topicName, consumer) {

    var deferred = $q.defer();

    consumeMessagesFromTopic(consumer, "instance", topicName, format).then(
      function success(things) {
        // var resultingTextData = "";
        if (format == "binary") {
          var data2 = angular.forEach(things, function (d) {
            d.key = $base64.decode(d.key);
            d.value = $base64.decode(d.value);
          });
          // resultingTextData = angular.toJson(data2, true);
        } else {
          // resultingTextData = angular.toJson(things, true);
        }

        // $log.info("COMPLETED entire object " + JSON.stringify(things));
        deferred.resolve(things);
      },
      function failure(message) {
        deferred.reject(message); // message is logged up-stream
      }
    );

    return deferred.promise;

  }

  $rootScope.allCurlCommands = "";

  // Factory should return
  return {

    // Proxy methods
    isNormalTopic: function (topicName) {
      return isNormalTopic(topicName);
    },
    shortenControlCenterName: function(topicName) {
      return shortenControlCenterName(topicName)
    },
    getBrokers: function () {
      return getBrokers();
    },
    bytesToSize: function (bytes) {
      return bytesToSize(bytes);
    },
    getTopicNames: function () {
      return getTopicNames();
    },
    deleteConsumerInstance: function (consumerName) {
      return deleteConsumerInstance(consumerName);
    },
    getNormalTopics: function (topicNames) {
      var normalTopics = [];
      angular.forEach(topicNames, function (topicName) {
        if (isNormalTopic(topicName)) {
          normalTopics.push(topicName)
        }
        if (normalTopics.toString().indexOf("Error in getting topics from kafka-rest") > -1) {
          $log.error("Error in getting topics from kafka-rest");
        }
      });
      return normalTopics;
    },
    getControlTopics: function (topicNames) {
      var controlTopics = [];
      angular.forEach(topicNames, function (topicName) {
        if (isControlTopic(topicName))
          controlTopics.push(topicName)
      });
      return controlTopics;
    },

    hasExtraConfig: function (topicName) {
      var extraTopicConfig = {};
      angular.forEach($rootScope.topicDetails, function (detail) {
        if (detail.name === topicName) {
          extraTopicConfig = detail.configs;
        }
      });
      return (JSON.stringify(extraTopicConfig).replace("{}", ""));
    },

    /**
     * Composite method, that fetches information about particular topics
     * and adds some enhanced metadata
     */
    getAllTopicInformation: function (topicNames) {

      var start = new Date().getTime();

      var topicsInformation = []; // Array of topic-information with enhanced metadata

      var deferred = $q.defer();
      var promises = topicNames.map(function (topicName) {
        return getTopicMetadata(topicName);
      });
      $q.all(promises).then(
        function success(topicMetadataArray) {

          // Add enhanced metadata: (shortName)
          angular.forEach(topicMetadataArray, function (topicMetadata) {
            topicsInformation.push(topicMetadata);
            // @see ### TOPIC-INFORMATION ### in SAMPLES.txt
          });
          $log.debug("  ..pipeline got " + topicMetadataArray.length + " topic metadata in [ " + (new Date().getTime() - start) + " ] msec");
          deferred.resolve(topicsInformation);
        },
        function failure(error) {

        });

      return deferred.promise;

    },

    getDataType: function (topicName) {
      var dataType = "..";
      // Check if we know the topic data type a priory
      if (KAFKA_REST_ENV.JSON_TOPICS.indexOf(topicName) > -1) {
        dataType = "json";
      } else if (KAFKA_REST_ENV.BINARY_TOPICS.indexOf(topicName.substring(0, 24)) > -1) {
        dataType = "binary";
      } else {
        // If topicDetails are not available wait
        angular.forEach($rootScope.topicDetails, function (detail) {
          if (detail.name === topicName) {
            angular.forEach(angular.fromJson(schemas), function (schema) {
              if ((schema.value != null) && (schema.value.subject != null) && (schema.value.subject == topicName + "-value")) {
                //$log.info("FOUND YOU !! " + topicName);
                dataType = "avro";
              }
            });
          }
        });
      }
      if (dataType == "") {
        $log.warn("Could not find the message type of topic [" + topicName + "]");
      }
      return dataType;
    },

    /**
     * Composite method.
     *
     *   Create avro|json|binary consumer
     *   Consume some data
     *   Delete consumer
     */
    consumeKafkaRest: function (format, topicName) {
      $rootScope.allCurlCommands = "";

      var start = (new Date()).getTime();
      var consumer = "Consumer-" + start;
      var consumerName = consumer + "-" + format;

      var deferred = $q.defer();
      createNewConsumer(consumer, consumerName, format, "smallest", true).then( // TODO (true), latest
        function success(data) {
          //data.instance_id + " base_uri = " + response.data.base_uri
          startFetchingData(format, topicName, consumerName).then(
            function success(data) {
              //$log.info("Consumed data -> " + data);
              // At the end .. let's see if we need to clean-up
              deleteConsumerInstance(consumerName).then(
                function success() {
                  $log.debug("  ..pipeline create-consume-delete in [ " + ((new Date()).getTime() - start) + " ] msec");
                  deferred.resolve(data);
                } // Failures are managed in the factory
              );
            } // Failures are managed in the factory
          )
        },
        function failure(response) {
          toastFactory.showSimpleToast(response);
        }
      );

      return deferred.promise;

    },

    //News
    loadSchemas: function () {
          var start = new Date().getTime();
          var schemasPromise = this.consumeKafkaRest("json", "_schemas");
          schemasPromise.then(function (allSchemas) {
             var end = new Date().getTime();
//            $rootScope.schemas = allSchemas;
             $log.info("[" + (end - start) + "] msec - to get " + angular.fromJson(allSchemas).length + " schemas from topic _schemas"); //  + JSON.stringify(allSchemas)
             schemas = allSchemas
             return schemas;
          }, function (reason) {
            $log.error('Failed: ' + reason);
          }, function (update) {
            $log.info('Got notification: ' + update);
          });
    }

  }

});
angularAPP.service('toastFactory', function ($rootScope, $mdToast) {

  var last = {
    bottom: false,
    top: true,
    left: false,
    right: true
  };

  var toastPosition = angular.extend({}, last);

  /* Public API of this factory*/
  this.getToastPosition = function () {
    this.sanitizePosition();

    return Object.keys(toastPosition)
      .filter(function (pos) {
        return toastPosition[pos];
      })
      .join(' ');
  };

  this.sanitizePosition = function () {
    var current = toastPosition;
    if (current.bottom && last.top) current.top = false;
    if (current.top && last.bottom) current.bottom = false;
    if (current.right && last.left) current.left = false;
    if (current.left && last.right) current.right = false;
    last = angular.extend({}, current);
  };

  this.showSimpleToast = function (message) {
    $mdToast.show(
      $mdToast.simple()
        .textContent(message)
        .position(this.getToastPosition())
        .hideDelay(20000)
    );
  };

  this.showActionToast = function (message) {
    var toast = $mdToast.simple()
      .textContent(message)
      .action('DELETE')
      .highlightAction(true)
      //.highlightClass('md-accent')// Accent is used by default, this just demonstrates the usage.
      .position(this.getToastPosition())
      .hideDelay(20000);

    $mdToast.show(toast).then(function (response) {
      if (response == 'ok') {
        //alert('You clicked the \'UNDO\' action.');
      }
    });
  };

  this.hideToast = function () {
    $mdToast.hide();
  };

}).config(function ($mdThemingProvider) {
  // $mdThemingProvider.theme('default')
  //   .primaryPalette('green')
  //   .accentPalette('red')
  //   .warnPalette('red');
  //
  // $mdThemingProvider.theme('tab')
  //   .primaryPalette('blue-grey')
  //   .accentPalette('blue-grey')
  //   .warnPalette('grey');
});
/**
 * Utils angularJS Factory
 */
angularAPP.factory('UtilsFactory', function ($log) {

  // Sort arrays by key
  function sortByKey(array, key, reverse) {
    return array.sort(function (a, b) {
      var x = a[key];
      var y = b[key];
      return ((x < y) ? -1 * reverse : ((x > y) ? 1 * reverse : 0));
    });
  }

  /* Public API */
  return {

    sortByKey: function (array, key, reverse) {
      return sortByKey(array, key, reverse);
    },
    sortByVersion: function(array) {
      var sorted = array.sort(function(a, b) {
        return a.version - b.version;
      });
      return sorted;
    },
    IsJsonString: function (str) {
      try {
        JSON.parse(str);
      } catch (e) {
        return false;
      }
      return true;
    }

  }

});
angularAPP.controller('KafkaTopicsConfigCtrl', function ($scope, $http, $log, KafkaRestProxyFactory) {

  $log.info("Starting kafka-topics controller : config");

  $scope.schemaRegistryURL = UI_SCHEMA_REGISTRY;
  $scope.kafkaRest = KAFKA_REST;
  $scope.brokers = {};
  $scope.connectionFailure = false;

  /**
   * At start up get the Brokers that the kafka-rest server is using
   */
  KafkaRestProxyFactory.getBrokers().then(
    function success(brokers) {
      $scope.brokers = brokers.brokers;
    },
    function failure() {
      $scope.connectionFailure = true;
    });

});

angularAPP.controller('HomeCtrl', function ($log, toastFactory) {
  $log.info("Starting kafka-topics controller : home");
  toastFactory.hideToast();
});
angularAPP.controller('KafkaTopicsListCtrl', function ($scope, $rootScope, $location, $routeParams, $mdToast, $log, KafkaRestProxyFactory, toastFactory) {

  $log.info("Starting kafka-topics controller : list (getting topic info)");
  toastFactory.hideToast();

  $rootScope.$watch('topicCategoryUrl' ,function(){
    $scope.displayingControlTopics = false;
    if ($rootScope.topicCategoryUrl =='c') {
      $scope.displayingControlTopics = true;
      }
    },true);

  KafkaRestProxyFactory.loadSchemas();

  /**
   * At start-up get all topic-information
   */
  KafkaRestProxyFactory.getTopicNames().then(
    function success(allTopicNames) {
      $scope.topics = KafkaRestProxyFactory.getNormalTopics(allTopicNames);;
      $scope.controlTopics = KafkaRestProxyFactory.getControlTopics(allTopicNames);;
      $rootScope.topicsCache = $scope.topics; //TODO do we need that??

      KafkaRestProxyFactory.getAllTopicInformation($scope.topics).then(
        function success(topicDetails) {
          $rootScope.topicDetails = topicDetails;

          // .. only then fetch [Control] topics info
          KafkaRestProxyFactory.getAllTopicInformation($scope.controlTopics).then(
            function success(controlTopicDetails) {
              $rootScope.controlTopicDetails = controlTopicDetails;
            });

        }, function failure(reason) {
          $log.error('Failed: ' + reason);
        });

      $scope.topicsPerPage = 7;

      $scope.controlTopicIndex = $scope.controlTopics.indexOf($rootScope.topicName );
      $scope.controlTopicPage = Math.ceil($scope.controlTopicIndex / $scope.topicsPerPage);
      if ($scope.controlTopicPage < 1) {
        $scope.controlTopicPage = 1
      }

      $scope.normalTopicIndex = $scope.topics.indexOf($rootScope.topicName );
      $scope.normalTopicPage = Math.ceil($scope.normalTopicIndex / $scope.topicsPerPage);
      if ($scope.normalTopicPage < 1) {
        $scope.normalTopicPage = 1
      }

    }, function (reason) {
      $log.error('Failed: ' + reason);
      toastFactory.showSimpleToast("No connectivity. Could not get topic names");
    }, function (update) {
      $log.info('Got notification: ' + update);
    });

  /**
   * View functions
   */

  $scope.getPartitionMessage = function (topicName) {
    return doCountsForTopic(topicName);
  };

  $scope.isNormalTopic = function (topicName) {
    return KafkaRestProxyFactory.isNormalTopic(topicName);
  };

  $scope.displayingControlTopics = $scope.isNormalTopic;

  $scope.hasExtraConfig = function (topicName) {
    return KafkaRestProxyFactory.hasExtraConfig(topicName);
  };

  $scope.getDataType = function (topicName) {
    return KafkaRestProxyFactory.getDataType(topicName);
  };

  $scope.shortenControlCenterName = function (topicName) {
    return KafkaRestProxyFactory.shortenControlCenterName(topicName);
  }

  $scope.listClick = function (topicName) {
    if (KafkaRestProxyFactory.isNormalTopic(topicName) == false) {
      $scope.CategoryTopicUrls = 'c';
    } else {
      $scope.CategoryTopicUrls = 'n';
    }
    $location.path("topic/" +  $scope.CategoryTopicUrls + "/" + topicName);
  }

  function doCountsForTopic(topicName) {
    var counts = {
        partitions : 0,
        replications : 0
    }

    angular.forEach($rootScope.topicDetails, function (topicDetail) {
      if (topicDetail.name == topicName) {
        counts.replications = topicDetail.partitions[0].replicas.length;
        counts.partitions = topicDetail.partitions.length;
      }
    });

    angular.forEach($rootScope.controlTopicDetails, function (topicDetail) {
      if (topicDetail.name == topicName) {
        counts.replications = topicDetail.partitions[0].replicas.length;
        counts.partitions = topicDetail.partitions.length;
      }
    });

    return doLalbels(counts.replications, 'Replication') + ' x ' + doLalbels(counts.partitions, 'Partition');
  }

  function doLalbels(count, name) {
    if (count == 0) return 'None ' + name;
    else if (count == 1) return '1 ' + name;
    else return count + ' ' + name +'s';
  }
});
angularAPP.controller('ViewTopicCtrl', function ($scope, $rootScope, $filter, $routeParams, $log, $mdToast, $location, $mdDialog, $http, KafkaRestProxyFactory, UtilsFactory) {

  $log.info("Starting kafka-topics controller : view ( topic = " + $routeParams.topicName + " )");
  $scope.topicName = $routeParams.topicName;
  $rootScope.topicName = $routeParams.topicName;

  $scope.topicCategoryUrl = $routeParams.topicCategoryUrl;
  $rootScope.topicCategoryUrl = $routeParams.topicCategoryUrl;

    if ($routeParams.selectedTabIndex == "topic") {
      $scope.selectedTabNnumber=0;
    }
    else if ($routeParams.selectedTabIndex == "table") {
      $scope.selectedTabNnumber=1;
    }
    else if ($routeParams.selectedTabIndex == "rawdata") {
      $scope.selectedTabNnumber=2;
    }
    else if ($routeParams.selectedTabIndex == "config") {
      $scope.selectedTabNnumber=3;
    }
    else {
      $scope.selectedTabNnumber=0;
    }

    $scope.onTabChanges = function(currentTabIndex){
        $location.path ("topic/" +  $scope.topicCategoryUrl + "/" + $scope.topicName + "/" + currentTabIndex, false);
    };

  $scope.showSpinner = true;
  $scope.KAFKA_TOPIC_DELETE_COMMAND = KAFKA_TOPIC_DELETE_COMMAND;

  /************* UI-GRID **************/
  $scope.gridOptions = {
    enableSorting: true,
    enableColumnResizing: true,
    // rowHeight: 3,
    columnDefs: [
      {field: 'offset', maxWidth: 75, cellClass: 'grid-center', headerCellClass: 'grid-header-landoop'},
      {field: 'partition', maxWidth: 75, cellClass: 'grid-center', headerCellClass: 'grid-header-landoop-small'},
      {field: 'key', cellClass: 'red', width: 150, headerCellClass: 'grid-header-landoop'},
      {
        field: 'value', headerCellClass: 'grid-header-landoop',
        cellTooltip: function (row, col) {
          return 'a' + row.entity.value;
        }
      }
    ]
  };
  // *********** UI- GRID **********

  $scope.topicType = KafkaRestProxyFactory.getDataType($scope.topicName);

  $scope.editor;

  $scope.aceLoaded = function (_editor) {
    $scope.editor = _editor;
    $scope.editor.$blockScrolling = Infinity;
    _editor.setOptions({
      minLines: 33,
      maxLines: 33
    });

  };

  $scope.isSchemaLong = function (schema) {
    return ((schema != null) && (schema.length >= 42))
  };

  $scope.getSchemaRegistryUrl = function (subject, version) {
    return UI_SCHEMA_REGISTRY + "/#/schema/" + subject + "/version/" + version;
  };

  $scope.getData = function (topicName) {
    $log.info("Download requested for " + $scope.aceString.length + " bytes ");
    var json = $scope.aceString;
    var blob = new Blob([json], {type: "application/json;charset=utf-8;"});
    var downloadLink = angular.element('<a></a>');
    downloadLink.attr('href', window.URL.createObjectURL(blob));
    downloadLink.attr('download', topicName + '.json');
    downloadLink[0].click();
  };

  // DIALOG //////
  var originatorEv;

  $scope.openMenu = function ($mdOpenMenu, ev) {
    originatorEv = ev;
    $mdOpenMenu(ev);
  };

  $scope.notificationsEnabled = true;
  $scope.toggleNotifications = function () {
    $scope.notificationsEnabled = !$scope.notificationsEnabled;
  };

  $scope.streamFromBeginning = function () {
    var kbytesFromBeginning = $mdDialog.alert()
      .title('Stream from beginning of topic')
      .textContent('Will fetch 100 KBytes of data from topic')
      .targetEvent(originatorEv)
      .clickOutsideToClose(true)
      .ok('Okay!');

    $mdDialog.show(kbytesFromBeginning).then(function () {
      $log.info("Streaming from beginning");
      $scope.consumeKafkaRest($scope.topicType, $scope.topicName);
    });

    originatorEv = null;
  };

  $scope.hasExtraConfig = function (topicName) {
    var extra = KafkaRestProxyFactory.hasExtraConfig(topicName);
    if (extra != '') {
      // $log.debug("Topic details " + topicName + " HAS EXTRA CONFIG " + extra);
    }
    return extra;
  };

  $scope.getExtraConfig = function (topicName) {
    var extra = KafkaRestProxyFactory.hasExtraConfig(topicName);
    return JSON.parse(extra);
  };

  $scope.getDefautConfigValue = function (configKey) {
    var defaultConfigValue = "";
    angular.forEach(KAFKA_DEFAULTS, function (kafkaDefault) {
      if (kafkaDefault.property == configKey) {
        defaultConfigValue = kafkaDefault.default;
      }
    });
    return defaultConfigValue;
  };

  $scope.getConfigDescription = function (configKey) {
    var configDescription = "";
    angular.forEach(KAFKA_DEFAULTS, function (kafkaDefault) {
      if (kafkaDefault.property == configKey) {
        configDescription = kafkaDefault.description;
      }
    });
    return configDescription;
  };

   $rootScope.showMoreDesc = false;
   $rootScope.ToggleMoreDesc = function () {
      $rootScope.showMoreDesc = !$rootScope.showMoreDesc;
   };

  $scope.streamInRealTime = function () {
    $log.info("Streaming in real time");
    // This never happens.
  };
  ///////////////////////

  $log.debug("topicType=" + JSON.stringify($scope.topicType));
  // If value exists in an array
  function isInArray(value, array) {
    return array.indexOf(value) > -1;
  }

  function setCustomMessage(rows) {
    var totalRows = 0;
    if ($scope.topicName == "_schemas") {
      totalRows = rows.length;
      $scope.customMessage = "Topic <b>_schemas</b> holds <b>" + totalRows + "</b> registered schemas for the <a href='" + UI_SCHEMA_REGISTRY + "' target='_blank'>schema-registry</a>"
    } else if ($scope.topicName == "connect-configs") {
      totalRows = rows.length;
      $scope.customMessage = "Topic <b>connect-configs</b> holds <b>" + $scope.getConnectors(rows, 'connector-').length + "</b> connector configurations" +
        " and <b>" + $scope.getConnectors(rows, 'task-').length + "</b> task configurations";
    } else if ($scope.topicName == "connect-offsets") {
      totalRows = rows.length;
      $scope.customMessage = "Topic <b>connect-offsets</b> holds the offsets of your connectors. Displaying <b>" + totalRows + "</b> rows";
    } else if ($scope.topicName == "connect-status") {
      totalRows = rows.length;
      // $scope.customMessage = "Topic <b>connect-status</b> holds <b>" + $scope.getCompactedConnectStatus(rows, 'RUNNING').length + "</b> RUNNING connectors";
      $scope.customMessage = "";
    } else {
      if (UtilsFactory.IsJsonString(rows)) {
        totalRows = JSON.parse(rows).length;
        $scope.customMessage = "Displaying " + totalRows + " rows ";// + KafkaRestProxyFactory.bytesToSize(rows.length);
      } else {
        totalRows = rows.length;
        $scope.customMessage = "Displaying " + totalRows + " rows ";// + KafkaRestProxyFactory.bytesToSize(rows.length);
      }
    }
    $scope.topicIsEmpty = totalRows == 0;

    $scope.gridOptions.data = rows; //TODO removeme
    return totalRows;
  }

  // text can be 'connector-' 'task-' 'commit-'
  $scope.getConnectors = function (rows, search) {
    var defaultValue = [];
    //$log.error(rows);
    if (rows != undefined) {
      angular.forEach(rows, function (row) {
        if (row.key.indexOf(search) == 0) {
          defaultValue.push(row);
        }
      });
    }
    return (defaultValue);
  };

  // Get the keys ..
  $scope.getTopicKeys = function (rows) {
    var allTopicKeys = ["key", "partition", "offset"];
    if (rows != undefined) {
      angular.forEach(angular.fromJson(rows), function (row) {
        // $log.info("data= " + JSON.stringify(row.value));
        if (JSON.stringify(row.value) != null && JSON.stringify(row.value).indexOf("{\\") == 0) {
          angular.forEach(JSON.parse(row.value), function (value, key) {
            //$log.info("Key-Value = " + key + " value=" + value);
            if (!isInArray(key, allTopicKeys)) {
              allTopicKeys.push(key);
            }
          });
        } else {
          // $log.info(" value=" + row.value);
          if (!isInArray("value", allTopicKeys)) {
            allTopicKeys.push("value");
          }
        }
        // TODO
      });
      // $log.info("Completeeeed " + JSON.stringify(rows).length);
      $scope.totalKeys = allTopicKeys.length;
    }
    // else {
    //   $log.debug("Undefined");
    // }
    return allTopicKeys;
  };

  $scope.getTopicValues = function (rows) {
    var allTopicValues = [];
    angular.forEach(angular.fromJson(rows), function (row) {
      // $log.debug(row + "    " + JSON.stringify(row.value));
      var x = {};
      x.key = row.key;
      x.partition = row.partition;
      x.offset = row.offset;
      if (JSON.stringify(row.value) != null && JSON.stringify(row.value).indexOf("{\"") == 0) {
        x.extraDataFlattened = [];
        // $log.error("Value is JSon->" + JSON.stringify(row.value));
        angular.forEach(row.value, function (peiler) {
          //$log.debug("peiler = " + peiler);
          x.extraDataFlattened.push(peiler);
        });
      } else {
        x.extraDataFlattened = [];
        // $log.info("Key= " + key + " value= " + value);
        if (row.value != undefined) {
          x.extraDataFlattened.push(row.value);
        }
      }
      allTopicValues.push(x);
    });
    // $log.debug("XXX " + JSON.stringify(allTopicValues));
    $scope.allTopicValues = allTopicValues;
    return allTopicValues;
  };

  $scope.getConnector = function (row) {
    if (row.value.length >= 5) {
      var data = JSON.parse(row.value).properties;
      var topics = "";
      if (data.topic != null) {
        topics = topics + data.topic;
      } else if (data.topics != null) {
        topics = topics + data.topics;
      }
      // TODO: This run's 10ns of times ! $log.error(data);
      var connectorData = {
        name: data.name,
        topic: topics,
        tasksmax: data['tasks.max'],
        file: data.file,
        class: data['connector.class']
      };
      return connectorData;
    }
  };

  $scope.getTask = function (row) {
    var data = JSON.parse(row.value).properties;
    var topics = "";
    if (data.topic != null) {
      topics = topics + data.topic;
    } else if (data.topics != null) {
      topics = topics + data.topics;
    }
    // TODO: This run's 10ns of times ! $log.error(data);
    var taskData = {
      topic: topics,
      file: data.file,
      class: data['task.class']
    };
    return taskData;
  };

  $scope.getCommit = function (row) {
    var data = JSON.parse(row.value);
    // TODO: This run's 10ns of times ! $log.error(data);
    var commitData = {
      tasks: data.tasks
    };
    return commitData;
  };

  $scope.isNormalTopic = function (topicName) {
    return ['_schemas', 'connect-status'].indexOf(topicName) == -1;
  };

  $scope.isControlTopic = function(topicName) {
     return !KafkaRestProxyFactory.isNormalTopic(topicName);
  };

  // At start-up this controller consumes data
  var start = new Date().getTime();
  if (($scope.topicType == "json") || ($scope.topicType == "binary") || ($scope.topicType == "avro")) {
    var dataPromise = KafkaRestProxyFactory.consumeKafkaRest($scope.topicType, $scope.topicName);
    dataPromise.then(function (allData) {
      var end = new Date().getTime();
      $log.info("[" + (end - start) + "] msec - to get " + angular.fromJson(allData).length + " " + $scope.topicType + " rows from topic " + $scope.topicName); //  + JSON.stringify(allSchemas)

      $scope.aceString = angular.toJson(allData, true);
      $scope.rows = allData;
      setCustomMessage($scope.rows);
      flattenTable(allData);
      $scope.getTopicValues($scope.rows);

      end = new Date().getTime();
      $log.info("[" + (end - start) + "] msec - to get & render"); //  + JSON.stringify(allSchemas)
      $scope.showSpinner = false;
    }, function (reason) {
      $log.error('Failed: ' + reason);
    }, function (update) {
      $log.info('Got notification: ' + update);
    });
  } else {
    $log.warn("We don't really know the data type of topic" + $scope.topicName + " so we will attempt all options..");
    // If we don't know we need to guess by trying Avro -> JSon -> Binary
    var dataPromiseAvro = KafkaRestProxyFactory.consumeKafkaRest("avro", $scope.topicName);
    dataPromiseAvro.then(function (allData) {
      if (JSON.stringify(allData).indexOf("error") > 0) {
        $log.warn('Failed with Avro - going to try with Json this time (' + allData + ')');
        var dataPromiseAvro = KafkaRestProxyFactory.consumeKafkaRest("json", $scope.topicName);
        dataPromiseAvro.then(
          function (allData) {
            if (JSON.stringify(allData).indexOf("error_code") > 0) {
              $log.warn('Failed with JSon as well - going to try with Binary this time (' + allData + ')');
              var dataPromiseAvro = KafkaRestProxyFactory.consumeKafkaRest("binary", $scope.topicName);
              dataPromiseAvro.then(function (allData) {
                $log.info("Binary detected");
                var end = new Date().getTime();
                $scope.topicType = "binary";
                $scope.aceString = angular.toJson(allData, true);
                $scope.rows = allData;
                setCustomMessage($scope.rows);
                flattenTable(allData);
                angular.fromJson($scope.rows);
                $scope.getTopicValues($scope.rows);
                $log.info("[" + (end - start) + "] msec - to get " + angular.fromJson(allData).length + " " + $scope.topicType + " rows from topic " + $scope.topicName); //  + JSON.stringify(allSchemas)
                $scope.showSpinner = false;
              }, function (reason) {
                $log.error('Failed with Binary as well ?! :(  (' + reason + ')');
              });
            } else {
              $log.info("JSon detected");
              var end = new Date().getTime();
              $scope.topicType = "json";
              $scope.aceString = allData;
              $scope.rows = allData;
              setCustomMessage($scope.rows);
              flattenTable(allData);
              $scope.getTopicValues($scope.rows);
              $log.info("[" + (end - start) + "] msec - to get " + angular.fromJson(allData).length + " " + $scope.topicType + " rows from topic " + $scope.topicName); //  + JSON.stringify(allSchemas)
              $scope.showSpinner = false;
            }
          }, function (reason) {
          });
      } else {
        // $log.info("Avro detected" + allData);
        var end = new Date().getTime();
        $scope.topicType = "avro";
        $scope.aceString = angular.toJson(allData, true);
        $scope.rows = allData;
        setCustomMessage($scope.rows);
        flattenTable(allData);
        $scope.getTopicValues($scope.rows);
        $log.info("[" + (end - start) + "] msec - to get " + angular.fromJson(allData).length + " " + $scope.topicType + " rows from topic " + $scope.topicName); //  + JSON.stringify(allSchemas)
        $scope.showSpinner = false;
      }
    }, function (reason) {
    });
  }

  //tODO
  $scope.myTopic = $filter('filter')($rootScope.topicsCache, {name: $scope.topicName}, true);

  ///////////////////////////////////////////
  $mdToast.hide();
  $scope.kafkaDefaults = KAFKA_DEFAULTS; //TODO
  $scope.topicsOn = true;
  $scope.zookeeperInfo = "zookeeper.landoop.com.info.goes.here";
  $scope.brokers = KAFKA_REST_ENV.BROKERS;

  $scope.changeView = function () {
    $scope.topicsOn = !$scope.topicsOn;
  };

  // 1. Create a consumer for Avro data, starting at the beginning of the topic's log.
  // 2. Then consume some data from a topic, which is decoded, translated to JSON, and included in the response.
  // The schema used for deserialization is fetched automatically from the schema registry.
  // 3. Finally, clean up.
  // [ avro | json | binary ]
  $scope.consumeKafkaRest = function (messagetype, topicName) {
    $scope.showSpinner = true;
    var dataPromise = KafkaRestProxyFactory.consumeKafkaRest(messagetype, topicName);
    dataPromise.then(function (data) {
        $scope.aceString = data;
        $scope.rows = data;
        $scope.showSpinner = false;
        setCustomMessage($scope.rows);
        flattenTable(data);
        $scope.getTopicValues($scope.rows);
      }, function (reason) {
        $log.error('Failed: ' + reason);
      }, function (update) {
        $log.info('Got notification: ' + update);
      }
    );
  };

  // TOPICS
  $rootScope.selectedTopic;
  $scope.selectTopic = function (topicObj) {
    $rootScope.selectedTopic = topicObj
  };

  $scope.getLeader = function (partitions) {
    if (partitions.length > 0) return partitions[0];
  };

  $scope.getTailPartitions = function (partitions) {
    return partitions.slice(1);
  };

  $scope.getKafkaDefaultValue = function (key) {
    var defaultValue;
    angular.forEach(KAFKA_DEFAULTS, function (item) {
      if (item.property == key) {
        defaultValue = item.default;
      }
    });
    return defaultValue;
  };

  $scope.getKafkaDefaultDescription = function (key) {
    var defaultValue;
    angular.forEach(KAFKA_DEFAULTS, function (item) {
      if (item.property == key) {
        defaultValue = item.description;
      }
    });
    return defaultValue;
  };

  // BROKERS
  $scope.selectedBroker;
  $scope.selectBroker = function (brokerObj) {
    $scope.selectedBroker = brokerObj
  };


  /**
   * TODO: Move to utils
   */

  // This one is called each time - the user clicks on an md-table header (applies sorting)
  $scope.logOrder = function (a) {
    $log.info("Ordering event " + a);
    sortSchema(a);
  };

  function sortSchema(type) {
    var reverse = 1;
    if (type.indexOf('-') == 0) {
      // remove the - symbol
      type = type.substring(1, type.length);
      reverse = -1;
    }
    // $log.info(type + " " + reverse);
    $scope.rows = UtilsFactory.sortByKey($scope.rows, type, reverse);
  }

   //TODO move to service
    var flattenObject = function(ob) {
    	var toReturn = {};

    	for (var i in ob) {
    		if (!ob.hasOwnProperty(i)) continue;

    		if ((typeof ob[i]) == 'object') {
    			var flatObject = flattenObject(ob[i]);

    			for (var x in flatObject) {
    				if (!flatObject.hasOwnProperty(x)) continue;
    				toReturn[i + '.' + x] = flatObject[x];
    			}

    		} else {
    			toReturn[i] = ob[i];
    		}
    	}
    	return toReturn;
    };

 function flattenTable(rows) {

        var extraColumnsNumberValue = 0;
        var extraColumnsNumberKey = 0;
        var rowWithMoreColumns;
        $scope.flatRows = [];
        if (rows.length > 0) {
            angular.forEach(rows, function (row) {
                  if (row.key == undefined || row.key == null) row.key = '';
                  if (row.value == undefined || row.value == null) row.value = '';

                  if(angular.isNumber(row.value) || angular.isString(row.value)) {
                        extraColumnsNumberValue = 0
                        extraColumnsNumberKey = 0
                        $scope.cols = Object.keys(flattenObject(row));
                        $scope.cols2 = [];
                        $scope.cols3 = [];
                  } else {
                        var flatValue = flattenObject(row.value);
                        var flatKey = flattenObject(row.key);
                        var rowExtraColumnsValues = Object.keys(flatValue).length;
                        var rowExtraColumnsKeys = Object.keys(flatKey).length;

                        if(extraColumnsNumberValue < rowExtraColumnsValues) {
                            extraColumnsNumberValue = rowExtraColumnsValues;
                            rowWithMoreColumns = row;
                        }

                        if(extraColumnsNumberKey < rowExtraColumnsKeys) {
                            extraColumnsNumberKey = rowExtraColumnsKeys;
                            rowWithMoreColumns = row;
                        }

                        var newRow = {
                            "offset" : rowWithMoreColumns.offset,
                            "partition" : rowWithMoreColumns.partition,
                            "key" : rowWithMoreColumns.key,
                            "value" : rowWithMoreColumns.value
                        }

                        $scope.cols =  Object.keys(flattenObject(newRow));
                        $scope.cols2 = Object.keys(flattenObject(newRow.value));
                        $scope.cols3 = Object.keys(flattenObject(newRow.key));
                  }

                  $scope.flatRows.push(flattenObject(row));

                });

                $scope.extraColsNumValues = extraColumnsNumberValue;
                $scope.extraColsNumKeys = extraColumnsNumberKey;

         $scope.paginationItems = 10;
         $scope.showHideAllButtonLabel = 'show ' + rows.length;
     }
}

 $scope.showTree = function (keyOrValue) {
    return !(angular.isNumber(keyOrValue) || angular.isString(keyOrValue) || (keyOrValue==null));
 }

});
