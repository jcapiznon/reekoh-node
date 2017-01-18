'use strict'

let gulp = require('gulp')
let mocha = require('gulp-mocha')
let nodemon = require('gulp-nodemon')
let standard = require('gulp-standard')

let paths = {
  js: ['*.js', '*/*.js', '*/**/*.js', '!node_modules/**'],
  tests: ['tests/*.test.js']
}

gulp.task('standard', function () {
  return gulp.src(paths.js)
    .pipe(standard())
    .pipe(standard.reporter('default', {
      breakOnError: true,
      quiet: true
    }))
})

gulp.task('watch', function () {
  gulp.watch(paths.js, ['standard'])
})

gulp.task('run-tests', function () {
  return gulp.src(paths.tests)
    .pipe(mocha({reporter: 'spec'}))
})

gulp.task('run', function () {
  nodemon({
    script: 'app.js',
    ext: 'js',
    watch: paths.js,
    ignore: ['node_modules', 'public', 'public/lib', '.idea', '.git'],
    restartable: 'rs'
  })
})

gulp.task('test', ['run-tests'])
gulp.task('default', ['run', 'watch'])
